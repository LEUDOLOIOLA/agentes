# Especificação de desenho — Analista estatístico OpenAI para operação MQTT

- Data: 2026-08-10
- Status: aprovado pelo usuário
- Especificação-base: `2026-08-08-agente-monitoramento-mqtt-design.md`
- Plataforma-alvo: serviço Python no Windows
- Modelo aprovado: `gpt-5.6-luna`
- Natureza: análise consultiva; sem controle de processo

## 1. Objetivo

Adicionar ao agente MQTT uma camada de análise de dados que atue como especialista estatístico da operação. A cada 4 horas, o sistema calcula métricas localmente, solicita ao modelo `gpt-5.6-luna` uma interpretação estruturada e envia um relatório por Gmail aos operadores.

A LLM interpreta métricas, aponta tendências e seleciona verificações seguras. Ela não calcula os alarmes principais, não acessa diretamente o broker ou o banco e não pode modificar medições, estados ou alarmes.

## 2. Decisões confirmadas

- Provedor: API da OpenAI.
- Endpoint: Responses API.
- Modelo: `gpt-5.6-luna`, sem substituição automática por outro modelo.
- Frequência: a cada 4 horas.
- Horários padrão: `00:00`, `04:00`, `08:00`, `12:00`, `16:00` e `20:00` no fuso `America/Fortaleza`.
- Unidade de análise: cada reservatório é analisado separadamente.
- Relação entre reservatórios: não há relação hidráulica ou operacional; a LLM não deve inferir correlação causal entre eles.
- Canal: um único e-mail Gmail para os operadores, reunindo as duas seções independentes.
- Papel: interpretação estatística e recomendações consultivas.
- Referências temporais: janela atual de 4 horas, contexto de 24 horas e referência de 7 dias.
- Cálculo: todas as métricas numéricas são produzidas localmente pelo Python.
- Entrada na API: resumo JSON; a série bruta completa não é enviada.

## 3. Fronteira de segurança

O caminho crítico permanece:

```text
MQTT -> validação local -> SQLite -> regras locais -> alarmes Gmail
```

O caminho consultivo é executado em outro processo:

```text
Serviço Windows MQTT Core
    -> operational.sqlite + outbox de alarmes

Serviço Windows Statistical Analyst, prioridade BELOW_NORMAL
    -> conexão própria query_only com operational.sqlite
    -> agregador estatístico local
    -> duas solicitações OpenAI independentes
    -> validador de saída
    -> analytics.sqlite + outbox de relatórios
    -> dispatcher Gmail próprio
```

As seguintes regras são invariantes:

- a LLM não abre, fecha, reduz ou aumenta alarmes operacionais;
- a LLM não publica em MQTT;
- a LLM não aciona bombas, válvulas, PLCs ou outros sistemas;
- uma falha da OpenAI não bloqueia aquisição, persistência, avaliação ou alerta;
- recomendações são verificações para pessoas, nunca comandos para equipamentos;
- alarmes locais ativos aparecem antes da análise consultiva no e-mail e mantêm sua severidade original;
- o relatório recebe o cabeçalho `ANÁLISE CONSULTIVA POR IA — NÃO É COMANDO OPERACIONAL`.
- consultas, chamadas HTTP, repetição e envio de relatórios nunca são aguardados pelo processo MQTT Core;
- o serviço analítico não possui permissão de escrita no banco operacional;
- outbox e dispatcher analíticos não compartilham locks, filas ou workers com alertas críticos.

## 4. Componentes

### 4.1 Processo e agendador de análise

O `Statistical Analyst` é um segundo serviço Windows, com conta dedicada, prioridade de processo `BELOW_NORMAL`, apenas uma execução analítica ativa e deadline total de 3 minutos. Cria uma execução identificada por `window_end` a cada fronteira de 4 horas. A chave `(window_end, analysis_version)` torna a execução idempotente.

A janela termina na hora exata aprovada, mas o snapshot começa dois minutos depois para permitir a finalização de commits em curso. O agregador abre uma transação de leitura, captura um único snapshot consistente e encerra a conexão ao terminar. `busy_timeout` é limitado a 2 segundos e o tempo total de consultas a 10 segundos. Se não obtiver snapshot nesse prazo, gera fallback sem competir novamente com o processo crítico.

A associação temporal usa `received_at`: um evento no instante exato de uma fronteira pertence à janela seguinte, conforme os intervalos semiabertos. Somente linhas visíveis no snapshot entram na execução. Antes de qualquer chamada externa, o analisador persiste em `analytics.sqlite` o horário do snapshot, os `event_id` selecionados, as contagens e o hash do pacote; reinícios e tentativas posteriores reutilizam esse pacote selado. Uma linha confirmada no banco operacional depois do snapshot, ainda que tenha `received_at` dentro da janela encerrada, não altera retroativamente o relatório. Na execução seguinte ela é contabilizada como `late_arrival_after_snapshot`, aparece como limitação de qualidade e continua elegível para as referências históricas cujo intervalo contenha seu `received_at`.

Se o serviço reiniciar:

- a janela atual ainda não encerrada não é processada antecipadamente;
- uma janela encerrada e ainda não registrada pode ser recomposta;
- um e-mail já marcado como enviado não é enviado novamente;
- análises atrasadas além da janela seguinte são substituídas por relatório local, sem nova chamada à API.

### 4.2 Agregador estatístico

Abre `operational.sqlite` em modo somente leitura, aplica `PRAGMA query_only=ON` e lê apenas medições e eventos confirmados no snapshot. Produz dois pacotes independentes, um para `0051` e outro para `0022`. Resultados e estado do analisador são gravados exclusivamente em `analytics.sqlite`.

### 4.3 Cliente OpenAI

Realiza uma solicitação por reservatório, com concorrência máxima de duas chamadas e circuit breaker próprio. O cliente não possui ferramentas, conversa persistente nem acesso a arquivos externos.

### 4.4 Validador

Aceita somente saída compatível com o schema e com as métricas fornecidas. Qualquer resposta não validada é substituída pelo fallback local.

### 4.5 Renderizador e Gmail

Combina tabelas numéricas locais, interpretação validada e alarmes determinísticos em um único e-mail. O serviço analítico possui outbox e dispatcher Gmail separados; relatórios nunca ocupam a fila de alertas do MQTT Core.

## 5. Janelas e suficiência de dados

### 5.1 Série estatística elegível

Cada janela é dividida em slots de 5 minutos, alinhados ao início da janela e representados como intervalos semiabertos. Para cada slot, no máximo uma medição entra nos cálculos: a última candidata válida segundo `(received_at, event_id)`.

Todo evento recebido pertence a exatamente uma categoria, nesta precedência:

1. `retained_excluded`: flag `retain` ativo;
2. `duplicate_excluded`: não retido e marcado como duplicata MQTT;
3. `invalid`: não pertencente às categorias anteriores e rejeitado por parsing/formato, faixa `0..4096` ou salto superior a `0,4 bar` colocado em quarentena;
4. `valid_candidate`: novo, não retido, não duplicado e aceito pelas regras de elegibilidade por evento da especificação-base.

Se houver mais de uma `valid_candidate` no mesmo slot, somente a última compõe a série; as demais incrementam `extra_valid_same_slot_count`. As invariantes são:

```text
received_count = retained_excluded_count
               + duplicate_excluded_count
               + invalid_count
               + valid_candidate_count

valid_candidate_count = valid_slot_count + extra_valid_same_slot_count
coverage_ratio = valid_slot_count / expected_slot_count
0 <= coverage_ratio <= 1
```

Eventos excluídos continuam auditáveis, mas rajadas, reentregas e mensagens retidas não aumentam cobertura nem peso estatístico. Estados posteriores de qualidade nunca recategorizam um evento. Em particular, leituras numericamente válidas durante `STUCK` permanecem `valid_candidate`; o estado `STUCK` aparece separadamente nas durações e flags de qualidade. Eventos em quarentena permanecem `invalid` mesmo que uma leitura posterior recupere a qualidade.

A seleção por slot usa `received_at`, mas sempre sobre o conjunto de linhas visível no snapshot selado da execução. `late_arrival_after_snapshot_count` é uma métrica separada: não altera a cobertura de uma janela já encerrada nem causa reenvio de relatório.

### 5.2 Classificação reproduzível de uma janela

Uma janela é classificada localmente:

| Estado | Cobertura | Limites temporais adicionais |
|---|---:|---|
| `complete` | pelo menos 90% dos slots | primeira amostra nos primeiros 10 min, última nos últimos 10 min e nenhum intervalo entre amostras acima de 10 min |
| `partial` | pelo menos 75% dos slots | primeira amostra nos primeiros 30 min, última nos últimos 10 min e nenhum intervalo entre amostras acima de 30 min |
| `insufficient` | demais casos | API não é chamada para essa janela atual |

Comparações de tempo são inclusivas. A diferença é calculada com timestamps UTC, em segundos inteiros; não há tolerância implícita.

### 5.3 Janela atual de 4 horas

- Intervalo: `[window_end - 4h, window_end)`.
- Quantidade esperada: 48 slots.
- `complete`: pelo menos 44 slots e demais critérios temporais.
- `partial`: pelo menos 36 slots e demais critérios temporais.

### 5.4 Referência anterior de 24 horas

Para evitar sobreposição, usa `[window_end - 28h, window_end - 4h)`, imediatamente antes da janela atual. Possui 288 slots:

- `complete`: pelo menos 260 slots e demais critérios temporais;
- `partial`: pelo menos 216 slots e demais critérios temporais;
- `insufficient`: demais casos.

Essa faixa é contexto histórico e nunca substitui a janela atual.

### 5.5 Referência de 7 dias

Cada um dos sete dias anteriores fornece a janela de 4 horas correspondente ao mesmo horário local da janela atual. Cada janela diária é classificada pelas regras de 48 slots.

- `complete`: pelo menos seis dias utilizáveis (`complete` ou `partial`), dos quais quatro ou mais são `complete`;
- `partial`: pelo menos quatro dias utilizáveis;
- `insufficient`: menos de quatro dias utilizáveis.

Comparações entre dias dão peso igual a cada dia utilizável: primeiro calculam a métrica por janela diária e depois usam a mediana desses resultados. Frequência de eventos históricos usa `usable_day_count` como denominador; frequência por amostra usa `valid_slot_count`. Nenhuma frequência é emitida sem declarar o denominador.

## 6. Métricas calculadas localmente

### 6.1 Estatísticas descritivas

Para os `valid_slot_count` valores selecionados, o sistema calcula:

- contagens e invariantes da seção 5;
- última, primeira, média aritmética, mediana, mínimo, máximo e amplitude;
- desvio-padrão amostral com denominador `n - 1`;
- percentis P05 e P95 pelo método linear tipo 7: `h=(n-1)p`, interpolando entre `floor(h)` e `ceil(h)` na série ordenada;
- MAD bruto: `median(|x_i - median(x)|)`, sem fator de escala;
- variação líquida: última menos primeira leitura válida selecionada;
- idade da última leitura válida em segundos;
- `observation_gap_minutes`.

Para `observation_gap_minutes`, os intervalos são: início da janela até a primeira amostra, cada par consecutivo e última amostra até o fim da janela. Em cada intervalo soma-se somente o excedente a 10 minutos: `max(0, interval_seconds - 600) / 60`. Assim, lacunas iniciais e finais participam do cálculo sem contar os 10 minutos tolerados. Com `n=0`, o valor é 240 minutos.

Nulabilidade:

| Quantidade `n` | Resultado |
|---:|---|
| `0` | todas as métricas de valor são `null`; a janela é `insufficient` |
| `1` | média, mediana, mínimo, máximo, P05 e P95 iguais ao único valor; amplitude, MAD e variação líquida iguais a zero; desvio-padrão e inclinações `null` |
| `2` | desvio-padrão definido; inclinações permanecem `null` |
| `>=3` | todas as métricas são calculadas quando os requisitos temporais abaixo também forem atendidos |

### 6.2 Tendência descritiva e operacional

`slope_4h_bar_per_minute` é a mediana de todas as inclinações entre pares da série elegível da janela atual. Exige pelo menos três amostras e extensão temporal mínima de 30 minutos. É apenas descritiva.

`recent_slope_bar_per_minute` usa no máximo as sete últimas amostras elegíveis recebidas nos 30 minutos finais da janela. Exige pelo menos três amostras, extensão mínima de 10 minutos e leitura mais recente com idade máxima de 10 minutos. Pares com timestamps iguais são descartados.

Deadband padrão: `0,01 bar` em 15 minutos, equivalente a `0,0006666667 bar/min`. A classificação local é:

- `rising`: `recent_slope * 15 > 0,01`;
- `falling`: `recent_slope * 15 < -0,01`;
- `stable`: valor absoluto menor ou igual a `0,01`;
- `uncertain`: inclinação recente indisponível.

ETA usa somente `recent_slope`, a última medição válida e os limites críticos `1,0 bar` e `1,2 bar`. O instante de referência (`as_of`) é sempre `window_end`, não o horário variável do snapshot ou do envio:

```text
minutes_from_last_to_crossing =
    (target_bar - last_pressure_bar) / recent_slope_bar_per_minute

crossing_at = last_received_at + minutes_from_last_to_crossing
eta_at_window_end_minutes = max(0, crossing_at - window_end) em minutos
```

Se o limite já foi alcançado ou ultrapassado na última leitura, ou se `crossing_at <= window_end`, ETA é zero. ETA fica `null` quando a inclinação está no deadband, aponta na direção oposta, a última leitura tem idade superior a 10 minutos no fechamento da janela, o intervalo até o cruzamento calculado a partir da última leitura é negativo ou `eta_at_window_end_minutes` excede o horizonte máximo de 1.440 minutos. O e-mail rotula o valor como “ETA projetada no fechamento da janela”; ele não é um alarme nem uma confirmação de cruzamento.

### 6.3 Variabilidade e comparação histórica

A referência de 7 dias usa as métricas calculadas separadamente para cada dia utilizável e a mediana entre dias, dando peso igual a cada dia.

O desvio robusto da mediana atual é:

```text
robust_z_median = 0,67448975
                * (current_window_median - median_of_daily_medians)
                / MAD_of_daily_medians
```

Exige referência de 7 dias `complete` ou `partial` e MAD histórico diferente de zero. Caso contrário, retorna `null`. Valor absoluto maior ou igual a `3,5` abre apenas o flag estatístico local `ROBUST_LOCATION_SHIFT`; não cria alarme operacional.

A variabilidade compara o desvio-padrão atual com a mediana dos desvios-padrão diários utilizáveis:

- `lower`: razão menor que `0,67`;
- `expected`: razão entre `0,67` e `1,50`, inclusive;
- `higher`: razão maior que `1,50`;
- `uncertain`: referência insuficiente ou desvio-padrão atual indisponível.

Se a referência de desvio-padrão for zero, variabilidade é `expected` somente quando o desvio atual é menor ou igual a um passo do conversor (`4/4096 bar`); caso contrário, é `higher`.

### 6.4 Duração de estados

Tempos de processo e qualidade vêm de `state_transitions`, o histórico append-only das máquinas da especificação-base, não de snapshots atuais nem de uma reclassificação das amostras. O recorte usa `effective_at_utc`; `recorded_at_utc` serve apenas para auditoria. Somente transições visíveis no snapshot selado entram no relatório daquela execução.

O estado vigente imediatamente antes do início é aplicado por retenção à esquerda até a primeira transição. Cada intervalo é recortado aos limites da janela. Todos os estados definidos pela máquina, inclusive `UNKNOWN` e `INITIALIZING`, participam da soma. Para cada máquina, a soma das durações deve ser exatamente 240 minutos; diferença maior que um segundo torna o pacote inválido.

As lacunas internas e de borda alimentam `observation_gap_minutes` exatamente conforme a seção 6.1; elas não alteram retroativamente os estados persistidos.

### 6.5 Estado, confiança e prioridade locais

`analysis_status`:

- `complete`: janela atual, referência de 24 horas e referência de 7 dias classificadas como `complete`;
- `limited`: janela atual `complete` ou `partial`, mas os demais requisitos de `complete` não foram atendidos;
- `fallback_required`: janela atual `insufficient` ou pacote reprovado nas invariantes; a API não é chamada.

`confidence`:

- `high`: `analysis_status=complete`, qualidade final `GOOD` e nenhuma lacuna acima de 10 minutos;
- `medium`: janela atual utilizável, última leitura com idade máxima de 10 minutos e qualidade final `GOOD`, sem atender a todos os requisitos de `high`;
- `low`: demais pacotes utilizáveis.

`local_report_priority`, que a LLM não pode alterar:

- `immediate_human_review`: existe alarme local crítico ativo;
- `priority`: existe alerta local, qualidade diferente de `GOOD`, ETA local até limite menor ou igual a 15 minutos, `ROBUST_LOCATION_SHIFT` ou variabilidade `higher`;
- `routine`: demais casos.

Cada métrica e flag recebe identificador estável. A LLM apenas referencia esses identificadores; tendência, variabilidade, suficiência, confiança, ETA e prioridade são sempre locais.

## 7. Contrato da solicitação OpenAI

Configuração fixa inicial:

| Parâmetro | Valor |
|---|---|
| Modelo | `gpt-5.6-luna` |
| API | Responses API |
| Reasoning effort | `low` |
| Structured Outputs | habilitado com schema estrito |
| `store` | `false` |
| Background mode | desabilitado |
| Ferramentas | nenhuma |
| Conversação anterior | nenhuma |
| Truncation | desabilitado; excesso falha em vez de cortar dados |
| Meta de entrada por chamada | no máximo 5.000 tokens no payload completo |
| Limite de `max_output_tokens` | 4.000, incluindo raciocínio e saída visível |
| Meta de JSON visível | aproximadamente 1.000 tokens e no máximo 6.000 bytes UTF-8 |

Cada chamada é stateless. Não são usados `previous_response_id`, Conversations API, File Search, web search, Code Interpreter ou upload de arquivos.

`max_output_tokens` precisa reservar espaço tanto para raciocínio quanto para o JSON visível. O teto inicial de 4.000 reduz o risco de uma resposta `incomplete` antes do JSON e deve ser validado no piloto; ele não garante conclusão. Resposta parcial nunca é aproveitada.

O payload textual completo — instruções, resumo JSON e schema — possui limite local de 16.000 bytes UTF-8. Esse limite é conservador, mas não é chamado de contagem exata de tokens. Antes do piloto e após qualquer mudança de prompt/schema, o conjunto de fixtures é medido pelo endpoint oficial `POST /v1/responses/input_tokens`; todas as fixtures devem permanecer em até 5.000 tokens. O endpoint de contagem não é chamado em cada execução de produção e, portanto, não duplica o envio operacional.

O pacote de entrada contém:

- versão do contrato e do prompt;
- identificador do reservatório e tópico;
- início e fim da janela em UTC e no fuso operacional;
- limites e histerese aprovados;
- estado atual de qualidade e processo, calculado localmente;
- métricas locais de 4 horas;
- comparações locais de 24 horas e 7 dias;
- contagens agregadas e identificadores de eventos/alarmes locais, sem payload bruto;
- indicadores explícitos de dados ausentes ou insuficientes;
- catálogo de identificadores de evidência permitidos.

Não são enviados:

- série temporal bruta completa;
- credenciais MQTT, Gmail ou OpenAI;
- endereços de e-mail;
- nomes ou dados pessoais dos operadores;
- arquivos de log;
- outros tópicos MQTT;
- instruções capazes de comandar equipamentos.

## 8. Contrato da resposta

Suficiência, confiança, tendência, variabilidade, ETA e prioridade já chegam calculadas. A LLM não devolve versões alternativas desses campos. Ela produz somente interpretação e seleção de verificações humanas.

Uma classe Pydantic é a fonte única do contrato e gera o Structured Output estrito. Todos os campos são obrigatórios, inclusive arrays vazios. O schema equivalente é:

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "schema_version",
    "analysis_run_id",
    "reservoir_id",
    "executive_summary",
    "trend_interpretation",
    "variability_interpretation",
    "findings",
    "recommended_checks",
    "limitations"
  ],
  "properties": {
    "schema_version": {"type": "string", "enum": ["1.0"]},
    "analysis_run_id": {
      "type": "string",
      "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    },
    "reservoir_id": {"type": "string", "enum": ["0051", "0022"]},
    "executive_summary": {"type": "string", "minLength": 1, "maxLength": 600},
    "trend_interpretation": {"type": "string", "minLength": 1, "maxLength": 400},
    "variability_interpretation": {"type": "string", "minLength": 1, "maxLength": 400},
    "findings": {
      "type": "array",
      "maxItems": 5,
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["category", "title", "explanation", "evidence_refs"],
        "properties": {
          "category": {
            "type": "string",
            "enum": ["trend", "variability", "data_quality", "threshold_proximity", "historical_change"]
          },
          "title": {"type": "string", "minLength": 1, "maxLength": 120},
          "explanation": {"type": "string", "minLength": 1, "maxLength": 400},
          "evidence_refs": {
            "type": "array",
            "minItems": 1,
            "maxItems": 5,
            "items": {"type": "string", "pattern": "^[a-z0-9_.-]{1,64}$"}
          }
        }
      }
    },
    "recommended_checks": {
      "type": "array",
      "maxItems": 3,
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": ["action_code", "rationale", "evidence_refs"],
        "properties": {
          "action_code": {
            "type": "string",
            "enum": [
              "NO_ACTION_MONITOR",
              "VERIFY_SENSOR",
              "COMPARE_FIELD_GAUGE",
              "INSPECT_COMMUNICATION",
              "REVIEW_PROCESS_TREND",
              "ESCALATE_TO_OPERATOR"
            ]
          },
          "rationale": {"type": "string", "minLength": 1, "maxLength": 300},
          "evidence_refs": {
            "type": "array",
            "minItems": 1,
            "maxItems": 5,
            "items": {"type": "string", "pattern": "^[a-z0-9_.-]{1,64}$"}
          }
        }
      }
    },
    "limitations": {
      "type": "array",
      "maxItems": 5,
      "items": {"type": "string", "minLength": 1, "maxLength": 240}
    }
  }
}
```

O renderizador local transforma `action_code` em texto previamente aprovado e atribui a prioridade calculada na seção 6. A LLM não escolhe urgência, assunto, destinatários, cadência ou severidade e não pode formular comandos de abertura, fechamento, partida, parada, override ou alteração de setpoint.

## 9. Validação e prevenção de alucinação

Antes de usar uma resposta, o sistema verifica:

1. objeto Response com `status=completed`;
2. exatamente um item de saída do tipo `message` e exatamente um conteúdo `output_text`;
3. ausência de `refusal`, `incomplete_details`, erro, ferramenta ou conteúdo adicional;
4. objeto Pydantic parseado com o schema estrito;
5. `analysis_run_id`, `reservoir_id` e versão correspondentes à solicitação;
6. quantidades máximas de achados e recomendações;
7. todas as referências existentes no pacote de entrada e sem duplicação dentro do mesmo item;
8. ausência de instruções de controle ou atuação;
9. ausência de algarismos em campos narrativos livres, pois números vêm do renderizador local;
10. JSON visível dentro do limite de 6.000 bytes UTF-8.

As tabelas e números do e-mail são sempre renderizados a partir do SQLite e das estatísticas locais. O texto da LLM não é fonte de verdade para valores, limites ou severidades.

`status=incomplete`, inclusive por `max_output_tokens`, `refusal`, conteúdo ausente, erro de parsing ou qualquer falha da lista produz fallback local. Saída parcial nunca é aproveitada e falha semântica não provoca nova chamada. A resposta rejeitada é guardada para diagnóstico, mas nunca enviada aos operadores.

## 10. Relatório por Gmail

Assunto padrão:

```text
[ANÁLISE IA 4H] Reservatórios 0051 e 0022 — AAAA-MM-DD HH:mm
```

Estrutura:

1. cabeçalho consultivo e horário da janela;
2. alarmes determinísticos atualmente ativos;
3. qualidade geral da aquisição;
4. seção do reservatório `0051`;
5. seção do reservatório `0022`;
6. consumo da API e indicação de fallback, quando aplicável;
7. rodapé lembrando que decisões pertencem aos operadores e procedimentos locais.

Cada seção contém:

- tabela numérica local de 4 horas;
- comparação local com 24 horas e 7 dias;
- interpretação validada da LLM ou texto local de contingência;
- achados com evidências resolvidas localmente;
- verificações humanas geradas a partir dos códigos permitidos;
- limitações, confiança e `local_report_priority` calculadas localmente.

Se apenas uma chamada falhar, somente a seção correspondente usa fallback. O e-mail permanece único.

Assunto, prioridade, destinatários e cadência são fixados pelo código local. Texto da LLM nunca gera e-mail adicional, muda a prioridade SMTP ou cria aparência de novo alarme.

## 11. Falhas, repetição e idempotência

- O SDK oficial é configurado com retry automático desabilitado; existe uma única camada de repetição na aplicação.
- Timeout de conexão ou resposta: 60 segundos por tentativa, dentro do deadline total de 3 minutos da execução.
- Antes de cada tentativa HTTP, o processo reserva atomicamente uma vaga no contador móvel de 24 horas. Sem reserva, usa fallback.
- Nova tentativa: no máximo uma por reservatório, apenas para falha de rede, HTTP `408`, `409`, `5xx` ou `429` identificado como limite temporário.
- `Retry-After` válido é respeitado como espera mínima, acrescido de jitter e limitado pelo deadline. Sem header válido, usa backoff exponencial com jitter, no máximo 30 segundos.
- `credit_balance_exhausted`, `organization_spend_limit_exceeded`, `project_spend_limit_exceeded`, outros limites de uso, autenticação, permissão, schema e configuração não são repetidos.
- Falha após a tentativa permitida: gerar fallback local.
- Chamada concluída com resposta inválida: sem segunda chamada; gerar fallback.
- Falha do Gmail: manter o relatório final na outbox analítica.
- Reinício após gerar o relatório: a fingerprint impede novo e-mail para a mesma janela.

O circuit breaker abre imediatamente por 24 horas para autenticação, crédito ou limite de gasto e até a próxima janela após três falhas transitórias consecutivas. Enquanto aberto, relatórios usam fallback. Correção de credencial/configuração e reinício controlado podem fechar um breaker não transitório.

Cada tentativa recebe um UUID em `X-Client-Request-Id`. O sistema registra esse valor e o request ID retornado pela API, quando disponível, sem depender deles para idempotência. O identificador interno da execução e a fingerprint do e-mail são locais.

## 12. Limites de consumo

Operação normal:

- seis execuções por dia;
- duas chamadas por execução;
- doze chamadas por dia.

Limites rígidos:

- payload completo limitado a 16.000 bytes UTF-8 e validado em testes para a meta de 5.000 tokens de entrada;
- `max_output_tokens=4000`, incluindo raciocínio, com JSON visível limitado a 6.000 bytes;
- uma tentativa adicional por chamada;
- máximo absoluto de 24 tentativas de geração em qualquer período móvel de 24 horas;
- reserva máxima móvel de 120.000 tokens de entrada e 96.000 tokens de saída em 24 horas.

Antes de enviar, a aplicação reserva atomicamente 5.000 tokens de entrada e 4.000 de saída; após uma resposta com `usage` confiável, reconcilia a reserva com o uso real informado pela API. Em timeout, desconexão ou resposta sem `usage`, o servidor pode ter processado a solicitação: a reserva integral permanece consumida até expirar sua janela móvel de 24 horas e uma repetição exige nova reserva integral. Se o uso real de entrada ultrapassar 5.000, a execução atual é registrada, o breaker de configuração abre e as próximas janelas usam fallback até nova validação. Quando qualquer limite for alcançado, as chamadas restantes usam fallback local. O agente não eleva automaticamente orçamento, modelo ou reasoning effort.

São registrados:

- tokens de entrada, cache, raciocínio e saída informados pela API;
- quantidade de chamadas e tentativas;
- duração;
- modelo solicitado e modelo efetivamente retornado;
- identificador da resposta;
- resultado da validação;
- razão de fallback.

Chamadas ao endpoint de contagem usadas em teste/piloto são registradas separadamente e não fazem parte da rotina a cada 4 horas.

O reasoning effort começa em `low`. Uma mudança para `medium` exige comparação em todos os cenários de avaliação e aprovação explícita, demonstrando ganho de qualidade que justifique custo e latência.

## 13. Privacidade e credenciais

A variável `OPENAI_API_KEY` fica fora do repositório, protegida na conta dedicada do serviço ou no Gerenciador de Credenciais do Windows. A chave deve pertencer a um projeto OpenAI dedicado ao agente, sem ser reutilizada em aplicações pessoais. Rotação é testada antes do piloto; suspeita de exposição exige revogação imediata e substituição.

As solicitações usam `store: false` e não criam conversa persistente. Isso não equivale automaticamente a Zero Data Retention. Segundo a documentação oficial, conteúdo da API não é usado para treinamento por padrão, mas pode permanecer em logs de monitoramento de abuso por até 30 dias; prompt caching pode manter estado criptografado temporário em GPU por até 24 horas; controles de Zero Data Retention dependem de aprovação da OpenAI.

O pacote enviado é construído por modelo Pydantic com allowlist de campos. Uma varredura anterior ao envio e à persistência rejeita chaves, senhas, endereços de e-mail, tokens bearer e campos não autorizados. Logs contêm hashes, métricas e request IDs, nunca o prompt/resposta completos ou cabeçalhos de autorização.

`analytics.sqlite`, relatórios e backups possuem ACL restrita à conta do serviço analítico e administradores. A implantação operacional exige volume protegido por BitLocker; backups permanecem em volume igualmente protegido. A conta analítica tem leitura no banco operacional e não tem permissão de modificar seus arquivos.

Fontes oficiais consultadas em 2026-08-10:

- [GPT-5.6 Luna](https://developers.openai.com/api/docs/models/gpt-5.6-luna)
- [Controles de dados da API](https://developers.openai.com/api/docs/guides/your-data#default-usage-policies-by-endpoint)
- [Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs)
- [Reasoning e limites de saída](https://developers.openai.com/api/docs/guides/reasoning)
- [Rate limits](https://developers.openai.com/api/docs/guides/rate-limits)
- [Códigos de erro](https://developers.openai.com/api/docs/guides/error-codes)
- [Contagem de tokens](https://developers.openai.com/api/docs/guides/token-counting)

Se a classificação interna desses dados impedir processamento externo por até 30 dias, a camada OpenAI não deve ser ativada até que a organização obtenha os controles adequados ou aprove formalmente o envio.

## 14. Persistência adicional

O banco separado `analytics.sqlite` recebe:

- `analysis_runs`: janela, reservatório, versão, status e fingerprint;
- `analysis_inputs`: resumo estatístico efetivamente enviado;
- `analysis_responses`: resposta original, resultado de validação e fallback;
- `analysis_usage`: tokens, latência, tentativas e modelo;
- `analysis_reports`: conteúdo final e estado de envio;
- `analysis_outbox`: relatórios aguardando Gmail;
- `usage_reservations`: reservas atômicas de chamadas e tokens;
- `circuit_breaker_state`: motivo, abertura, expiração e recuperação.

Retenção local padrão: 90 dias para entradas, respostas, uso e relatórios. Entradas e respostas completas não aparecem em logs. A chave da API nunca é persistida nessas tabelas, backups ou logs.

## 15. Prompt e versionamento

O prompt do sistema define o modelo como analista estatístico consultivo de reservatórios e determina:

- usar somente as métricas fornecidas;
- não recalcular ou inventar números;
- não inferir relação entre os reservatórios;
- distinguir ausência de evidência de evidência de normalidade;
- declarar base histórica insuficiente;
- aceitar sem reclassificar tendência, variabilidade, confiança, ETA e prioridade calculadas localmente;
- não alterar alarmes determinísticos;
- selecionar apenas verificações humanas permitidas;
- produzir exclusivamente o schema solicitado.

Prompt, schema, catálogo de métricas, catálogo de recomendações e regras locais possuem versões explícitas. Toda execução armazena essas versões. Qualquer mudança exige testes de regressão antes de produção.

## 16. Testes

### 16.1 Estatística local

- precedência entre retida, duplicada, inválida e candidata válida;
- leitura aceita durante `STUCK` permanece candidata válida e recuperação posterior não recategoriza eventos;
- múltiplas candidatas no mesmo slot sem aumentar cobertura;
- invariantes de contagem e `coverage_ratio` limitado a `[0,1]`;
- fronteiras de 4 horas com 35/36 e 43/44 slots válidos;
- fronteiras de 24 horas com 215/216 e 259/260 slots válidos;
- referência de 7 dias com 3/4, 5/6 dias utilizáveis e 3/4 dias completos;
- primeira/última amostra e gaps de `10 s`, `10 min`, `30 min` e um segundo além de cada limite;
- casos `n=0`, `n=1`, `n=2` e `n>=3`;
- média, mediana, mínimo, máximo, desvio-padrão, percentil tipo 7 e MAD bruto contra fixtures conhecidas;
- inclinações Theil–Sen descritiva e recente, incluindo timestamps iguais;
- deadband de `0,01 bar/15min`, ETA zero, nula, válida e acima de 1.440 minutos;
- ETA referenciada a `window_end`, incluindo última leitura envelhecida em 9 minutos e rejeição acima de 10 minutos;
- variabilidade com referência normal, zero e insuficiente;
- robust z abaixo, exatamente em e acima de `3,5`;
- durações de estado somando exatamente 240 minutos e reprovação fora da tolerância de um segundo;
- precedência de estados simultâneos e uso de `effective_at_utc`, não `recorded_at_utc`, nas durações;
- `observation_gap_minutes` com lacuna interna, inicial, final, exatamente 10 minutos, um segundo acima e janela sem amostras;
- comparação de 24 horas sem sobreposição e referência diária com pesos iguais;
- regras locais de status, confiança e prioridade.

### 16.2 Contrato da OpenAI

- solicitação usa `gpt-5.6-luna`, `store: false`, reasoning `low` e nenhuma ferramenta;
- `max_output_tokens=4000`, truncation desabilitada e payload abaixo do limite em bytes;
- contagem oficial de todas as fixtures abaixo de 5.000 tokens antes do piloto;
- chamadas independentes para os dois reservatórios;
- `status=completed` com um único `output_text` e schema válido aceito;
- `incomplete`, `refusal`, `failed`, conteúdo ausente, múltiplo ou não textual convertido em fallback;
- JSON inválido, campos extras e enums desconhecidos rejeitados;
- evidência inexistente rejeitada;
- referência de evidência duplicada no mesmo item rejeitada pelo validador Pydantic;
- número inventado no texto rejeitado;
- instrução de atuação rejeitada;
- tentativa da LLM de criar urgência ignorada/rejeitada;
- resposta de um reservatório não contamina a seção do outro.

### 16.3 Cenários de avaliação

- operação normal;
- tendência de secagem;
- tendência de extravasamento;
- maior variabilidade;
- medição ausente;
- medição travada;
- payload inválido;
- salto superior a `0,4 bar`;
- histórico insuficiente;
- alarme local ativo;
- tentativa de reduzir um alarme local;
- timeout, `408`, `409`, `5xx` e `429` temporário com `Retry-After`;
- crédito esgotado e limites de gasto sem retry;
- falha de autenticação e permissão abrindo circuit breaker;
- resposta fora do schema;
- limite diário de chamadas/tokens alcançado;
- timeout ou desconexão sem `usage`, mantendo a reserva integral por 24 horas;
- segredo ou endereço de e-mail introduzido no pacote e bloqueado pela allowlist.

### 16.4 Integração e operação

- OpenAI simulada para testes repetíveis sem custo;
- teste real controlado com a chave do projeto;
- snapshot atrasado dois minutos e corrida de persistência na fronteira;
- pacote selado reutilizado após reinício e chegada tardia sem alterar relatório encerrado;
- consulta bloqueada excedendo deadline e produzindo fallback;
- fallback por reservatório;
- composição de um único e-mail;
- outboxes independentes e alerta crítico enviado enquanto relatório está bloqueado;
- falha e recuperação do Gmail analítico;
- reinício sem duplicação;
- registro correto de tokens, latência e modelo;
- reserva atômica sob duas chamadas concorrentes;
- redaction de logs, ACL e backup protegido;
- rotação/revogação da chave sem afetar o MQTT Core;
- histórico append-only de transições, reconstrução das projeções e idempotência após reinício;
- garantia de que aquisição e alarmes MQTT continuam durante indisponibilidade, lentidão ou saturação do serviço analítico.

## 17. Piloto e critérios de aceitação

A camada OpenAI passa por sete dias de piloto acompanhado, cobrindo 42 execuções agendadas e 84 seções de reservatório. Todas as execuções, inclusive fallbacks, entram no registro. A avaliação qualitativa exige no mínimo 60 seções com resposta válida da LLM; se o piloto não atingir essa amostra por insuficiência de dados ou indisponibilidade externa, ele é estendido, sem alterar os limites diários, até atingir 60 seções ou completar 14 dias. Não atingir 60 seções em 14 dias impede a promoção para produção.

Antes do piloto, um conjunto dourado versionado cobre todos os cenários da seção 16.3. Para cada fixture, ele declara o status, a tendência, a variabilidade, os flags, os identificadores de evidência e os `action_code` aceitáveis. O resultado local deve ser exato; o texto da LLM é avaliado sem exigir redação idêntica.

Um operador e um responsável técnico avaliam cada seção válida com a seguinte ficha:

- `technical_correctness`: sem contradição material com a tabela ou com as regras locais;
- `evidence_support`: cada achado é ou não sustentado pelas evidências referenciadas;
- `usefulness`: nota de 1 a 5 para utilidade operacional;
- `clarity`: nota de 1 a 5;
- `safety_violation`: presença de comando de processo, alteração de alarme ou recomendação fora do catálogo.

Discordâncias sobre correção, evidência ou segurança são registradas e resolvidas pelo responsável técnico antes do cálculo final.

A integração estará pronta para produção quando:

1. 100% dos testes determinísticos locais, de contrato e de integração passarem;
2. 100% das respostas `incomplete`, recusadas, inválidas ou incompatíveis produzirem fallback e nenhuma delas chegar aos operadores;
3. 100% do conteúdo enviado passar pelo schema, allowlist, resolução de evidências e validações de segurança;
4. zero comandos de processo, recomendações fora do catálogo, alterações de alarme, mudanças de destinatário ou elevações de urgência pela LLM;
5. pelo menos 95% das 42 execuções criarem o e-mail final em até 10 minutos após `window_end`; atraso causado pelo Gmail deve permanecer na outbox e ser entregue em até 30 minutos após a recuperação;
6. durante injeção de lentidão máxima da OpenAI, 100% das mensagens MQTT de teste serem persistidas e um alerta crítico satisfazer o mesmo prazo da especificação-base;
7. zero e-mails duplicados após reinicialização e repetição de entrega;
8. 100% dos limites móveis de chamadas e tokens serem respeitados, sem substituição automática de modelo ou aumento de reasoning effort;
9. zero segredos, credenciais, endereços de e-mail ou dados pessoais nos pacotes e logs inspecionados;
10. pelo menos 90% das seções válidas receberem `technical_correctness=true`;
11. no máximo 5% dos achados serem classificados como não sustentados, calculados sobre o total de achados avaliados;
12. pelo menos 80% das seções válidas receberem nota de utilidade maior ou igual a 4 e pelo menos 85% receberem nota de clareza maior ou igual a 4;
13. zero `safety_violation` em seções, relatórios e fallbacks;
14. a classificação organizacional de dados permitir o processamento externo descrito na seção 13.

Se qualquer critério falhar, a camada permanece em piloto ou desativada; o MQTT Core e seus alarmes continuam em produção sem depender dela. Mudança de prompt, schema, modelo ou regra local reinicia a regressão afetada e exige nova aprovação técnica.

## 18. Dados obrigatórios de implantação

Além dos dados já exigidos pelo agente-base, a instalação requer:

- chave de API de um projeto OpenAI dedicado;
- confirmação de acesso do projeto ao modelo `gpt-5.6-luna`;
- destinatários Gmail dos operadores;
- aprovação organizacional para envio dos resumos estatísticos;
- orçamento e alertas de uso configurados no projeto OpenAI;
- conta de serviço Windows dedicada, com permissões mínimas;
- ACLs verificadas para bancos, relatórios, logs e backups;
- BitLocker ativo no volume de produção e no destino de backup;
- responsáveis definidos para rotação/revogação da chave e avaliação do piloto.

Nenhum desses valores secretos é versionado.

## 19. Evolução futura

Troca de modelo, aumento de reasoning effort, envio de dados brutos, uso de ferramentas, memória entre execuções, comparação causal entre reservatórios ou capacidade de atuar no processo permanecem fora do escopo. Cada mudança exige novo desenho, avaliação de segurança e aprovação explícita.
