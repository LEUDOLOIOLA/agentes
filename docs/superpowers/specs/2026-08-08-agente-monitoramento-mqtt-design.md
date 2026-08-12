# Especificação de desenho — Agente de monitoramento MQTT de reservatórios

- Data: 2026-08-08
- Status: aprovado pelo usuário
- Plataforma-alvo: Windows, em operação contínua
- Natureza: supervisão e alertas; sem comando de bombas, válvulas ou PLC

## 1. Objetivo

Criar um agente local que assine continuamente dois tópicos MQTT de pressão de reservatórios, mantenha histórico auditável, avalie as condições operacionais a cada 15 minutos e envie alertas por Gmail. O agente deve antecipar risco de secagem ou extravasamento e detectar medição ausente, travada ou inválida.

Tópicos monitorados:

- `EPZ/META/0051/ANI01/RAW`
- `EPZ/META/0022/ANI01/RAW`

Os dois reservatórios usam as mesmas escalas e regras, mas mantêm estados e históricos independentes.

## 2. Escopo e limites de segurança

O agente é exclusivamente supervisório. Sua credencial MQTT deve permitir somente assinatura dos tópicos definidos. Ele não publica comandos nem aciona equipamentos.

Intertravamentos, chaves físicas de nível e proteções no PLC continuam sendo a barreira primária. O agente não pode garantir proteção durante falha total do computador, energia, rede, broker ou Gmail. A ausência do e-mail diário de integridade serve como indicação operacional de que o próprio monitor pode estar indisponível.

Ficam fora do primeiro escopo:

- comando automático de bombas ou válvulas;
- painel web ou aplicativo móvel;
- alta disponibilidade com segundo computador;
- integração com canais diferentes de Gmail.

A análise estatística consultiva pela API da OpenAI é especificada separadamente em `2026-08-10-openai-analista-estatistico-design.md`. Ela não altera os limites de segurança nem a natureza supervisória deste agente.

## 3. Requisitos confirmados

### 3.1 Aquisição

- O payload é um valor numérico bruto, como `272`.
- A faixa instrumental válida é de `0` a `4096`.
- `0` representa `0 bar` e `4096` representa `4 bar`.
- A publicação esperada ocorre a cada 5 minutos.
- O agente permanece conectado continuamente; não se conecta apenas a cada 15 minutos.
- Aquisição, validação, estados e alarmes usam regras determinísticas locais e não dependem de modelos de linguagem. A camada consultiva OpenAI executa separadamente a cada 4 horas e consome tokens apenas nessas análises.
- Cada mensagem recebe um timestamp UTC de chegada gerado pelo agente.
- O flag MQTT `retain`, quando disponível, é armazenado. Uma mensagem retida recebida na conexão inicial não comprova atividade recente, não atualiza o prazo de ausência e não abre nem encerra alarmes de processo; ela pode ser exibida apenas como último estado não confirmado.

### 3.2 Conversão

Fórmula:

```text
pressao_bar = valor_bruto / 4096 * 4
pressao_bar = valor_bruto / 1024
```

Exemplos e referências:

| Valor bruto | Pressão |
|---:|---:|
| 272 | 0,265625 bar |
| 1024 | 1,0 bar |
| 1228,8 | 1,2 bar |

O cálculo usa precisão de ponto flutuante. A comparação de limites é feita em bar, sem arredondar previamente para exibição. Quando o equipamento publicar somente inteiros, `1229` é o primeiro valor bruto que atende ao limite alto de `1,2 bar`.

### 3.3 Limites operacionais

| Condição | Regra de entrada |
|---|---|
| Atenção por nível baixo | pressão menor ou igual a `1,05 bar`, ou previsão de alcançar `1,0 bar` nos próximos 15 minutos |
| Risco de secar | pressão menor ou igual a `1,0 bar` |
| Atenção por nível alto | pressão maior ou igual a `1,15 bar`, ou previsão de alcançar `1,2 bar` nos próximos 15 minutos |
| Risco de extravasar | pressão maior ou igual a `1,2 bar` |

Leituras críticas válidas são avaliadas e notificadas imediatamente. A análise consolidada, inclusive qualidade e tendência, ocorre em janelas alinhadas aos minutos `00`, `15`, `30` e `45` do relógio local configurado.

## 4. Arquitetura

```text
Broker MQTT
    -> receptor contínuo
    -> decodificador e validador
    -> normalizador bruto/bar
    -> SQLite
    -> motor de regras imediato e periódico
    -> outbox persistente
    -> Gmail
```

O núcleo de aquisição e alarmes será um único serviço Python para Windows, dividido internamente em componentes pequenos e testáveis.

Um segundo serviço Windows, executado em processo separado e com prioridade inferior, consome o banco operacional somente para leitura e produz análises consultivas pela OpenAI. Ele usa banco, outbox e dispatcher Gmail próprios. Indisponibilidade, atraso ou resposta inválida desse serviço não interfere na aquisição MQTT, nas avaliações de 15 minutos nem nos alarmes locais. O contrato completo está na especificação relacionada de 2026-08-10.

### 4.1 Receptor MQTT

Responsabilidades:

- conectar usando identidade estável;
- assinar exatamente os dois tópicos;
- solicitar QoS 1;
- usar sessão persistente quando o broker permitir;
- reconectar com espera exponencial e variação aleatória;
- preservar tópico, bytes brutos, horário de chegada, QoS e flag `retain`;
- informar o estado da conexão ao monitor de integridade.

QoS 1 oferece entrega pelo menos uma vez, não exatamente uma vez. Portanto, persistência e alertas devem ser idempotentes. O QoS efetivo também depende do QoS usado pelo publicador.

TLS e validação de certificado são obrigatórios na implantação operacional. Uma opção sem TLS pode existir somente para testes locais explicitamente identificados. A conta MQTT deve possuir ACL de leitura restrita aos dois tópicos.

### 4.2 Decodificador, validador e normalizador

O decodificador aceita um escalar numérico em texto UTF-8, removendo apenas espaços externos. São inválidos:

- payload vazio;
- texto não numérico;
- `NaN` ou infinito;
- valor abaixo de `0` ou acima de `4096`;
- conteúdo que não corresponda ao formato escalar esperado.

O registro bruto é preservado mesmo quando inválido. Apenas leituras válidas atualizam o último nível confiável.

### 4.3 Persistência SQLite

O SQLite opera em modo WAL e contém, no mínimo:

- `raw_events`: envelope MQTT e resultado da decodificação;
- `measurements`: valores válidos normalizados;
- `quality_state`: projeção do estado de qualidade atual por reservatório;
- `process_state`: projeção do estado de processo atual por reservatório;
- `state_transitions`: histórico append-only das duas máquinas de estado;
- `alarms`: abertura, mudança de severidade, lembretes e recuperação;
- `outbox`: e-mails aguardando envio ou nova tentativa;
- `evaluations`: janelas já processadas e versão das regras.

Cada avaliação possui chave única por reservatório, fim da janela e versão da regra. Cada evento notificável possui fingerprint estável, evitando duplicidade após reconexão ou reinício.

Retenção padrão:

- eventos e medições: 90 dias;
- transições de estado: 365 dias;
- alarmes e recuperações: 365 dias;
- backups diários do banco: últimos 30 arquivos.

Os prazos são configuráveis, mas esses valores são o padrão operacional aprovado.

Cada linha de `state_transitions` contém, no mínimo: `transition_id`, `reservoir_id`, `machine` (`quality` ou `process`), `previous_state`, `new_state`, `effective_at_utc`, `recorded_at_utc`, `rule_version`, `trigger_kind`, `trigger_event_id` opcional e `evaluation_id` opcional. `effective_at_utc` representa quando a condição passou a valer; `recorded_at_utc`, quando o serviço confirmou e persistiu a transição. Os timestamps são UTC. Uma chave de idempotência estável impede repetir a mesma transição após reinício.

O histórico não é atualizado ou apagado antes do prazo de retenção. `quality_state` e `process_state` são projeções reconstruíveis a partir da última transição, não substitutos do histórico. Na primeira ativação do monitoramento, são gravadas as transições iniciais `<none> -> INITIALIZING` e `<none> -> UNKNOWN`, com `effective_at_utc` igual ao início da ativação. Reinícios recuperam as últimas transições e não criam novos estados iniciais.

### 4.4 Motor de regras

Há duas máquinas de estado independentes por reservatório.

Qualidade do dado:

```text
INITIALIZING -> GOOD
GOOD <-> MISSING | STUCK | INVALID | COMMUNICATION_LOST
```

Condição do processo:

```text
UNKNOWN -> NORMAL
NORMAL <-> LOW_WARNING <-> DRY_RISK
NORMAL <-> HIGH_WARNING <-> OVERFLOW_RISK
```

Uma falha de qualidade nunca encerra um alarme de processo. Nesse caso, o último estado do processo permanece aberto com a indicação de que não está sendo confirmado por medição confiável.

Quando condições de qualidade coexistem, a precedência é:

```text
COMMUNICATION_LOST > MISSING > INVALID > STUCK > GOOD > INITIALIZING
```

`INITIALIZING` é usado somente antes de existir contexto suficiente para classificar a qualidade. `MISSING` representa a falta de mensagem no prazo; tráfego inválido recente impede `MISSING`, mas mantém `INVALID`. A severidade de ausência aos 30 minutos pertence ao alarme associado e não cria outro nome de estado.

O instante efetivo de uma transição é determinístico:

- condição causada por mensagem: `received_at` do evento que completa a regra;
- `MISSING`: instante da última mensagem nova mais 15 minutos; sem mensagem desde a ativação, usa o horário da ativação como origem;
- `COMMUNICATION_LOST`: início da desconexão mais 5 minutos;
- `STUCK`: o mais tardio entre a primeira leitura da sequência mais 30 minutos e a sexta mensagem nova igual;
- recuperação com duas confirmações: `received_at` da segunda confirmação;
- recuperação de comunicação: `received_at` da primeira leitura nova e válida após a reconexão.

Uma avaliação pode ocorrer depois desse instante; nesse caso, preserva o `effective_at_utc` calculado e usa seu horário de persistência como `recorded_at_utc`. Quando uma condição prioritária termina, o novo estado é a condição ainda ativa de maior precedência. A máquina de processo só transita por uma leitura confiável ou por confirmações de histerese; enquanto a qualidade não estiver `GOOD`, ela mantém o último estado.

## 5. Regras de qualidade da medição

### 5.1 Ausência

- `MISSING/WARNING`: nenhuma mensagem nova recebida por mais de 15 minutos.
- `MISSING/CRITICAL`: nenhuma mensagem nova recebida por mais de 30 minutos.
- Mensagens inválidas não contam como nova medição confiável, embora comprovem que há tráfego MQTT.

### 5.2 Travamento

Uma medição entra em `STUCK` quando:

- o valor bruto permanece exatamente igual durante pelo menos 30 minutos;
- existem pelo menos seis mensagens novas com esse valor dentro do período;
- o receptor MQTT e o relógio do agente estão saudáveis.

A recuperação exige duas medições válidas consecutivas com alteração do valor bruto. As duas confirmações devem estar separadas por pelo menos 4 minutos, e uma entrega marcada pelo MQTT como duplicada não conta como nova confirmação.

### 5.3 Medição errada

Uma medição entra em quarentena e não atualiza o nível confiável quando:

- falha em qualquer validação de formato ou faixa; ou
- difere em mais de `0,4 bar` da última medição válida recebida dentro dos 15 minutos anteriores.

A primeira ocorrência abre um alerta de medição inválida. Ocorrências consecutivas ou 15 minutos sem uma nova leitura confiável agravam a severidade. Uma leitura suspeita isolada nunca é usada para limpar ou inverter um alarme de processo.

A elegibilidade é decidida por evento e não muda retroativamente. São rejeitados como medição apenas: falha de parsing/formato, valor fora de `0..4096` e salto superior a `0,4 bar` colocado em quarentena. Um estado de qualidade posterior não recategoriza eventos anteriores.

Leituras numericamente válidas e sem salto durante `STUCK` continuam armazenadas em `measurements`, marcadas com o estado de qualidade vigente e podem compor estatísticas descritivas. Elas não são usadas para limpar, reduzir ou inverter alarmes de processo enquanto `STUCK` estiver ativo. A recuperação exige as duas confirmações da seção 5.2; somente após a segunda a máquina volta a `GOOD` e o nível volta a ser confirmado para decisões de recuperação. `INVALID` termina ao chegar uma medição aceita, sujeita à precedência das demais condições ainda ativas.

### 5.4 Perda de comunicação

- Desconexão MQTT por até 5 minutos: registrar e tentar reconectar.
- Desconexão acima de 5 minutos: abrir alerta de comunicação.
- Reconexão: registrar recuperação, mas aguardar uma leitura nova e válida antes de declarar a qualidade como `GOOD`.

Se a conexão com a internet também impedir o Gmail, os e-mails permanecem na outbox até que o envio volte a funcionar.

## 6. Tendência, confirmação e histerese

A tendência usa pelo menos três medições válidas, cobrindo no mínimo 10 minutos. A inclinação é calculada pelo estimador de Theil–Sen, isto é, a mediana das inclinações entre pares de leituras recentes. Uma atenção antecipada é aberta quando a inclinação aponta para um limite e a projeção indica cruzamento em até 15 minutos.

Uma tendência não pode transformar uma leitura inválida em válida nem encerrar um alarme crítico.

Sempre que uma regra exigir duas leituras de confirmação, elas devem estar separadas por pelo menos 4 minutos. Uma reentrega MQTT marcada como duplicada é persistida para auditoria, mas não satisfaz uma confirmação adicional.

Histerese padrão:

| Estado ativo | Condição mínima para recuperação ou redução |
|---|---|
| `DRY_RISK` | duas leituras válidas consecutivas acima de `1,03 bar` |
| `LOW_WARNING` | duas leituras válidas consecutivas em ou acima de `1,07 bar`, sem tendência descendente perigosa |
| `OVERFLOW_RISK` | duas leituras válidas consecutivas abaixo de `1,17 bar` |
| `HIGH_WARNING` | duas leituras válidas consecutivas em ou abaixo de `1,13 bar`, sem tendência ascendente perigosa |

Quando um risco crítico reduz de severidade, o motor reavalia imediatamente se a faixa de atenção ainda se aplica. A recuperação total só ocorre quando a condição correspondente de atenção também foi superada.

## 7. Avaliação temporal

O receptor processa todas as mensagens no momento da chegada. O agendador executa avaliações nas fronteiras de 15 minutos e registra cada execução de forma idempotente.

Para cada reservatório, uma avaliação:

1. verifica a saúde do receptor e do broker;
2. busca mensagens e medições da janela;
3. atualiza a qualidade;
4. usa como valor operacional a medição confiável mais recente e calcula a tendência somente com medições aceitas pelas regras por evento;
5. atualiza o estado do processo;
6. grava transições e eventos de alarme;
7. insere notificações necessárias na outbox.

Após reinício, o agente recompõe janelas ainda não registradas sem reenviar notificações com fingerprints já processados.

## 8. Notificações por Gmail

O envio usa a conta Gmail configurada para o serviço. O segredo deve ser uma senha de app ou outro mecanismo aprovado pelo Google para SMTP; nunca a senha normal da conta, e nunca deve ser salvo no repositório.

Cada mensagem contém:

- severidade e tipo do alarme;
- identificador do reservatório e tópico;
- valor bruto e pressão, quando disponíveis;
- horário da leitura e idade da última leitura válida;
- razão objetiva da classificação;
- ação sugerida ao operador;
- identificador do alarme para auditoria.

Política:

- risco crítico válido: envio imediato;
- atenção: envio na avaliação de 15 minutos;
- aumento de severidade: envio imediato;
- persistência: lembrete a cada 60 minutos;
- recuperação: envio após duas leituras válidas consecutivas que atendam à histerese;
- e-mail diário de integridade: padrão às 08:00 no fuso `America/Fortaleza`;
- falha de envio: tentativas com atraso crescente, limitado a 60 minutos entre tentativas.

O destinatário, a conta remetente e a credencial são dados obrigatórios de implantação, fornecidos na instalação e mantidos fora do controle de versão.

## 9. Serviço Windows e observabilidade

O processo é instalado como serviço com:

- inicialização automática atrasada;
- recuperação automática após falha;
- diretórios explícitos para banco, backups e logs;
- encerramento controlado para concluir transações;
- logs estruturados com rotação;
- verificação de integridade interna.

Métricas mínimas registradas nos logs e no banco:

- estado MQTT e quantidade de reconexões;
- mensagens por tópico;
- idade da última mensagem e da última medição válida;
- contagem de payloads inválidos;
- horário e resultado da última avaliação;
- quantidade de alarmes abertos;
- tamanho e idade da outbox;
- horário do último e-mail enviado.

O agente valida toda a configuração antes de iniciar. Configuração inválida impede a inicialização e deixa uma mensagem clara no log de eventos/arquivo de log.

## 10. Configuração e segredos

O arquivo de configuração versionável contém somente valores não secretos:

- tópicos;
- escala;
- limites e histerese;
- frequências e janelas;
- tempos de ausência e travamento;
- retenção;
- horários de lembrete e integridade.

São dados obrigatórios fornecidos durante a instalação:

- host, porta e versão do broker;
- material de confiança TLS;
- usuário/senha ou certificado de cliente MQTT;
- endereço Gmail remetente;
- destinatário ou lista de destinatários;
- segredo de autenticação do Gmail.

Segredos ficam em variáveis protegidas da conta do serviço ou no Gerenciador de Credenciais do Windows. Um arquivo `.env` real não é versionado. O projeto fornece apenas um exemplo sem valores secretos.

## 11. Tratamento de erros

- Falha transitória de rede: reconectar/repetir com espera exponencial e jitter.
- Falha de parsing: preservar o evento bruto, classificar como inválido e continuar o receptor.
- Banco indisponível ou corrompido: interromper a aceitação de novas mensagens, registrar erro crítico e deixar o serviço reiniciar; nunca fingir que o dado foi persistido.
- Falha do Gmail: manter a outbox e continuar a aquisição MQTT.
- Relógio alterado: usar UTC internamente e impedir avaliações duplicadas pela chave idempotente.
- Encerramento ou reinício: restaurar estados, alarmes e outbox do SQLite.

## 12. Plano de verificação do desenho

### 12.1 Testes unitários

- conversão `272 -> 0,265625 bar`;
- conversão dos limites `1024 -> 1,0 bar` e `1228,8 -> 1,2 bar`, incluindo `1229` como primeiro bruto inteiro acima do limite alto;
- inclusão exata nos limites baixo e alto;
- histerese e duas leituras de recuperação;
- tendência com cruzamento previsto em até 15 minutos;
- payload vazio, texto, `NaN`, infinito e valor fora da faixa;
- salto acima de `0,4 bar` em 15 minutos;
- ausência aos 15 e 30 minutos;
- valor inalterado por 30 minutos com pelo menos seis mensagens;
- dado inválido não limpando alarme de processo;
- precedência de condições simultâneas de qualidade;
- `effective_at_utc` exato para timeout, travamento e recuperação, mesmo quando a avaliação ocorre depois;
- leitura aceita durante `STUCK` persistida sem limpar ou inverter alarme de processo;
- recuperação posterior sem recategorizar eventos históricos.

### 12.2 Testes de integração

- publicação nos dois tópicos por broker MQTT de teste;
- QoS 1 com duplicata entregue;
- mensagem retida na conexão inicial sem renovar a atividade nem alterar alarmes;
- desconexão, reconexão e retomada da sessão;
- criação e recuperação das janelas de 15 minutos;
- persistência e restauração após reinício;
- histórico append-only de transições, reconstrução das projeções e idempotência após reinício;
- indisponibilidade temporária do Gmail e drenagem posterior da outbox;
- limpeza por retenção e criação/rotação de backups.

### 12.3 Teste operacional no Windows

- iniciar automaticamente após reinicialização;
- reiniciar após encerramento inesperado;
- manter banco, logs e alarmes entre reinícios;
- enviar alerta, lembrete, recuperação e e-mail diário de integridade;
- confirmar que a credencial MQTT não consegue publicar comandos.

## 13. Critérios de aceitação

O sistema estará pronto para piloto quando:

1. receber e persistir ambos os tópicos continuamente por pelo menos 24 horas;
2. converter corretamente bruto para bar;
3. detectar em testes todos os estados de nível e qualidade definidos;
4. não limpar alarmes com leitura inválida ou ausente;
5. não duplicar e-mails após reconexão ou reinício;
6. recuperar e-mails pendentes após falha do Gmail;
7. reiniciar automaticamente com o Windows;
8. manter as credenciais fora do repositório;
9. passar por um teste acompanhado, sem atuação automática, antes do uso operacional.

## 14. Evolução futura

Se o número de tópicos, operadores ou exigências de disponibilidade crescer, os componentes podem ser separados em ingestor, avaliador e notificador, substituindo SQLite por PostgreSQL. Um segundo host e um mecanismo de liderança seriam necessários para tolerar falha da máquina. Essas mudanças não fazem parte do piloto atual.
