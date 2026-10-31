# Especificação de mudança — Execução em VPS Linux com Claude Code

- Data: 2026-10-31
- Status: proposta, aguardando aprovação do usuário
- Especificações afetadas:
  - `2026-08-08-agente-monitoramento-mqtt-design.md` (MQTT Core)
  - `2026-08-10-openai-analista-estatistico-design.md` (analista estatístico)
- Planos afetados:
  - `../plans/2026-08-12-mqtt-core-implementation-plan.md`
  - `../plans/2026-08-12-statistical-analyst-claude-implementation-plan.md` (antigo `...-openai-...`, renomeado e reescrito)
- Natureza: mudança de plataforma e de provedor da camada consultiva; regras de domínio inalteradas

## 1. Decisões confirmadas

| Tema | Antes | Depois |
|---|---|---|
| Local de execução | computador Windows local | VPS Linux na internet |
| Gerência dos processos | serviços Windows (`pywin32`) | unidades `systemd` |
| Broker MQTT | acessado pela rede local/internet | público, acessado pela VPS com TLS e usuário/senha |
| Papel do Claude Code | — | (1) ferramenta de desenvolvimento e manutenção na VPS; (2) motor da análise consultiva de 4 horas, em modo headless |
| Camada consultiva | OpenAI Responses API, `gpt-5.6-luna` | Claude Code headless (`claude -p`), modelo Claude fixo |
| Alarmes | regras determinísticas em Python | **sem mudança**: continuam em Python, sem LLM |

Tudo o que não está descrito aqui continua valendo como nas especificações originais: tópicos, conversão, limites, histerese, regras de qualidade, avaliação de 15 minutos, política de e-mail, contrato estatístico, validação anti-alucinação e fallback local.

## 2. Princípio mantido: LLM fora do caminho crítico

O caminho crítico continua determinístico:

```text
Broker MQTT público (TLS)
    -> mqtt-core.service (Python, systemd)
    -> operational.sqlite + outbox de alarmes
    -> Gmail SMTP
```

O Claude Code **não** recebe as mensagens MQTT, não decide alarmes e não fica em execução contínua. Motivos:

- alarmes precisam ser reproduzíveis e auditáveis;
- uma indisponibilidade do serviço de IA, ou o fim da cota, não pode silenciar um risco de secar ou extravasar;
- uma chamada a cada 5 minutos, por dois tópicos, 24 horas por dia, consumiria cota sem ganho de segurança.

Todos os invariantes da seção 3 da especificação do analista continuam valendo, trocando "OpenAI" por "Claude".

## 3. Plataforma VPS

### 3.1 Sistema e recursos

- Sistema: Ubuntu Server 24.04 LTS (ou outra distribuição LTS com `systemd` ≥ 255).
- Recursos mínimos: 2 vCPU, 2 GB de RAM e 20 GB de disco. O Claude Code roda em Node.js e só fica ativo durante a análise ou o desenvolvimento.
- Python: CPython 3.12 em `venv` dedicado por serviço.
- Relógio: `systemd-timesyncd` ou `chrony` ativo e sincronizado. Sem sincronismo, o monitor de integridade sinaliza relógio não saudável, e as regras de travamento (seção 5.2 da especificação-base) ficam suspensas.
- Fuso do sistema: UTC. O alinhamento operacional continua usando `ZoneInfo("America/Fortaleza")` dentro da aplicação.

### 3.2 Usuários e diretórios

| Usuário Linux | Uso | Acesso |
|---|---|---|
| `agente-mqtt` | `mqtt-core.service` | leitura/escrita em `/var/lib/agente-mqtt` |
| `agente-analista` | `statistical-analyst.service` e o Claude Code headless | leitura de `operational.sqlite` (grupo `agente-mqtt-ro`); leitura/escrita em `/var/lib/agente-analista` |
| usuário humano (ex.: `leudo`) | Claude Code interativo, desenvolvimento, `git` | sem acesso aos segredos de produção; usa `sudo` para implantar |

Nenhum serviço roda como `root`. Os usuários de serviço não têm shell de login (`/usr/sbin/nologin`).

Diretórios, que substituem os caminhos `C:\ProgramData\AgenteMQTT\...`:

```text
/opt/agente-mqtt/           código instalado (somente leitura para serviços)
/etc/agente-mqtt/           TOML não secreto (core.toml, analyst.toml)
/etc/credstore/             segredos, 0600 root, entregues via LoadCredential
/var/lib/agente-mqtt/       operational.sqlite, WAL, estado
/var/lib/agente-analista/   analytics.sqlite, relatórios, diretório de trabalho vazio do Claude
/var/backups/agente-mqtt/   cópias locais antes do envio externo
/var/log/agente-mqtt/       logs estruturados (também enviados ao journald)
```

### 3.3 Unidades systemd

`mqtt-core.service`:

- `Type=notify` com `WatchdogSec=60`. A aplicação envia `READY=1` após o catch-up e `WATCHDOG=1` a partir do laço principal saudável.
- `Restart=always`, `RestartSec=5`, `StartLimitIntervalSec=0`.
- `After=network-online.target time-sync.target`.
- `TimeoutStopSec=30`, para concluir transações no encerramento controlado.
- Endurecimento: `ProtectSystem=strict`, `ProtectHome=yes`, `PrivateTmp=yes`, `NoNewPrivileges=yes`, `ReadWritePaths=/var/lib/agente-mqtt /var/log/agente-mqtt`, `LoadCredential=` para os segredos MQTT e SMTP.
- `Nice=0`, com prioridade de E/S normal.

`statistical-analyst.service`:

- Serviço **contínuo** (`Type=notify`, `WatchdogSec=300`, `Restart=on-failure`). O agendador interno de 4 horas (fronteira + 2 minutos, com recomposição de janelas perdidas) e o dispatcher Gmail, que reenvia por até 60 minutos, ficam no processo. Um timer oneshot foi descartado na revisão dos planos porque encerraria o processo com retries de e-mail pendentes.
- O prazo de 3 minutos por execução continua sendo imposto pelo coordenador.
- `Requires=mqtt-core.service` e `After=mqtt-core.service`.
- `Nice=10`, `IOSchedulingClass=idle` e `CPUWeight=20`, que equivalem ao `BELOW_NORMAL` do Windows; `MemoryMax=1G`.
- `ReadOnlyPaths=/var/lib/agente-mqtt` e `ReadWritePaths=/var/lib/agente-analista`. A leitura vem do grupo `agente-mqtt-ro`.
- `KillMode=control-group`, para que nenhum processo `claude` sobreviva ao stop.
- Sem `MemoryDenyWriteExecute`, porque o Node.js do Claude Code precisa de JIT.

### 3.4 Segredos

Substituem o Gerenciador de Credenciais do Windows:

- Os arquivos ficam em `/etc/credstore/` (dono `root`, modo `0600`) e chegam ao processo por `LoadCredential=`. A aplicação os lê em `$CREDENTIALS_DIRECTORY`.
- Quando disponível, usar `systemd-creds encrypt` para cifrar os segredos em repouso.
- Segredos: usuário/senha MQTT, CA do broker (se for privada), senha de app do Gmail e credencial do Claude (seção 4.4).
- Nenhum segredo fica em TOML, log, Git, histórico do shell ou no diretório de trabalho do Claude Code.
- O `WindowsCredentialProvider` dos planos é substituído por `SystemdCredentialProvider`.

### 3.5 Rede e endurecimento da VPS

- Firewall (`ufw` ou `nftables`): entrada bloqueada por padrão, liberando só SSH. Nenhuma porta de aplicação fica aberta, porque o agente é apenas cliente MQTT e SMTP.
- SSH só com chave, sem login de `root` por senha; `fail2ban` ou equivalente.
- Saídas necessárias:
  - porta TLS do broker (normalmente `8883`);
  - SMTP do Gmail (`587` STARTTLS ou `465`);
  - `443` para a API da Anthropic e para atualizações.
- **Pendência de implantação:** confirmar que o provedor da VPS não bloqueia saída SMTP. Alguns provedores bloqueiam essas portas por padrão.
- Atualizações de segurança automáticas (`unattended-upgrades`). Reinicializações do kernel ficam em janela agendada e são registradas.
- TLS do broker: validação de certificado e hostname obrigatória, mínimo TLS 1.2. A ACL da conta MQTT continua restrita a *subscribe* nos dois tópicos.

### 3.6 Cifragem e backup

O BitLocker é substituído por:

- LUKS no volume de dados, quando o provedor permitir. Caso contrário, cifragem de disco oferecida pelo provedor, registrada como limitação aceita.
- Backup diário com `sqlite3 .backup`, cifrado (por exemplo, `restic` ou `age`) e enviado para **fora da VPS**. Retenção mínima de 30 dias.
- Teste de restauração antes do piloto e depois a cada trimestre.

## 4. Claude Code na VPS

### 4.1 Uso interativo (desenvolvimento e manutenção)

- Instalado para o usuário humano, não para os usuários de serviço.
- Usado para implementar os planos, rodar os testes, investigar logs (`journalctl -u mqtt-core`) e propor correções.
- Não recebe permissão para ler `/etc/credstore/` nem para escrever em `/var/lib/agente-*`. Implantação em produção só por script revisado (`scripts/deploy.sh`) e executado com `sudo` por uma pessoa.
- Alterações no código seguem o fluxo dos planos: teste, commit e implantação. Nunca edição direta em `/opt/agente-mqtt`.

### 4.2 Uso headless (analista de 4 horas)

O orquestrador continua sendo o Python do `statistical_analyst`. Ele calcula as métricas, sela o pacote, valida a saída e gera o fallback. Só a etapa "cliente OpenAI" (seção 4.3 da especificação do analista) é trocada por um `ClaudeCodeClient`, que executa um subprocesso por reservatório:

```text
claude -p
  --model <modelo-aprovado>
  --output-format json
  --json-schema <schema versionado>
  --system-prompt-file <prompt versionado>
  --tools ""
  --strict-mcp-config
  --setting-sources ""
  --disable-slash-commands
  --no-session-persistence
  --max-budget-usd <limite por chamada>
```

Regras do subprocesso:

- A entrada é o pacote JSON selado, via `stdin`. Nada é passado por argumento de linha de comando que apareça em `ps`.
- **Sem ferramentas** (`--tools ""`), sem servidores MCP, sem skills, sem hooks e sem configurações de projeto. O modelo não consegue ler arquivos, executar comandos nem acessar banco, broker ou rede. Isso reforça a fronteira da seção 3 do analista.
- O diretório de trabalho é `/var/lib/agente-analista/claude-empty/`, vazio e sem `CLAUDE.md`.
- O ambiente do processo é mínimo: só `PATH`, `HOME`, a credencial do Claude e variáveis de proxy quando houver.
- O Python impõe timeout de 60 s por chamada. Ao estourar, mata o grupo de processos.
- Fica proibido usar `--fallback-model`: o modelo é fixo, como já era exigido para `gpt-5.6-luna`.
- A saída JSON do Claude Code (`result`, `structured_output`, `usage`, `total_cost_usd`, `session_id`, `is_error`) é gravada em `analytics.sqlite` para auditoria. **Toda** a validação da seção 9 do analista continua sendo aplicada localmente, mesmo com `--json-schema`.
- Qualquer `is_error`, saída fora do schema, timeout ou código de saída diferente de zero gera fallback local, como antes.

### 4.3 Modelo

- Modelo padrão proposto: `claude-sonnet-5-5`, um bom equilíbrio de custo e qualidade para interpretar métricas já calculadas.
- O modelo é fixado em configuração versionada. Trocar de modelo reinicia a regressão do conjunto dourado (seção 17 do analista) e exige nova aprovação.
- "Reasoning effort" passa a ser o nível de esforço/raciocínio do modelo Claude. Ele começa no mínimo aceitável e só sobe com a mesma regra de comparação e aprovação.

### 4.4 Autenticação e custos

Há duas opções:

1. **Chave de API da Anthropic (recomendada para produção):** workspace dedicado no Console, com limite de gasto mensal e alertas. A variável `ANTHROPIC_API_KEY` chega por `LoadCredential=`, só ao `statistical-analyst.service`.
2. **Assinatura Claude (o login atual):** possível no piloto, com token gerado por `claude setup-token`. Antes, verificar se os termos do plano permitem uso automatizado e contínuo. O consumo disputa a mesma cota do uso interativo de desenvolvimento, e o fim da cota leva ao fallback.

Limites de consumo (seção 12 do analista), adaptados:

- Continuam: 6 execuções/dia, 2 chamadas por execução, 1 tentativa extra por chamada e máximo de 24 tentativas em 24 h móveis.
- A reserva de tokens passa a ser controlada por `--max-budget-usd` por chamada e por um orçamento diário em USD em `analytics.sqlite`, reconciliado com `total_cost_usd` e `usage`.
- A meta de entrada de 5.000 tokens é medida em testes com o endpoint oficial de contagem de tokens da Anthropic (`/v1/messages/count_tokens`). O prompt de sistema efetivo do Claude Code headless entra nessa medição.

### 4.5 Privacidade

A seção 13 do analista é reescrita para a Anthropic:

- Os dados enviados continuam sendo apenas o resumo estatístico com allowlist de campos e varredura de segredos. A série bruta não é enviada.
- Antes de ativar a camada, registrar as regras de retenção e de uso de dados aplicáveis ao tipo de conta escolhido (API comercial ou assinatura), conforme a documentação oficial vigente da Anthropic.
- Se a classificação interna dos dados não permitir esse processamento externo, a camada consultiva fica desativada. O MQTT Core continua em produção normalmente.

## 5. Disponibilidade

- Continua existindo um único host, sem alta disponibilidade (fora do escopo, como antes).
- A VPS remove a dependência da energia e da rede do local físico, mas cria dependência do provedor e do broker público.
- O e-mail diário de integridade às 08:00 continua sendo o sinal de vida.
- Recomendação adicional (opcional): um *heartbeat* externo, um serviço de "dead man's switch" que recebe um ping a cada 15 minutos e avisa por outro canal quando o ping para. Isso cobre a queda da VPS, que hoje só seria percebida às 08:00 do dia seguinte. Fica como decisão pendente do usuário.

## 6. Impacto nos planos de implementação

Status: aplicado nos dois planos em 2026-10-31.

### 6.1 MQTT Core

| Item do plano | Mudança |
|---|---|
| Tech stack | remover `pywin32`; `tzdata` continua opcional; adicionar notificação systemd (implementação própria via socket `NOTIFY_SOCKET`, sem dependência) |
| `secrets.py` / `WindowsCredentialProvider` | substituir por `SystemdCredentialProvider` (lê `$CREDENTIALS_DIRECTORY`) |
| `config/mqtt-core.example.toml` | caminhos Linux da seção 3.2 |
| `scripts/*.ps1` | substituir por `scripts/install-mqtt-core.sh`, `uninstall-mqtt-core.sh`, `verify-mqtt-core.sh` e pelas unidades em `deploy/systemd/` |
| Task 15 (serviço Windows) | vira "Serviço systemd, instalação e segurança operacional"; testes operacionais com `systemd-analyze verify` e smoke test com `systemctl` |
| Comandos `.venv\Scripts\python.exe` | `.venv/bin/python` |
| Teste com CWD `C:\Windows\System32` | trocar por CWD `/` |
| Runbook do piloto | "reinício Windows" vira reinício da VPS; incluir teste de bloqueio SMTP do provedor e de restauração do backup externo |

### 6.2 Statistical Analyst

| Item do plano | Mudança |
|---|---|
| Tech stack | remover `openai` e `pywin32`; adicionar o Claude Code CLI (versão fixada no script de instalação) |
| Cliente OpenAI | substituir por `ClaudeCodeClient` (subprocesso da seção 4.2) atrás da mesma interface, para que o validador e o fallback não mudem |
| Agendador interno | mantido no processo contínuo (ver 3.3); só a gerência do processo passa ao `systemd` |
| Contabilidade de tokens | adaptar para `usage` e `total_cost_usd` do Claude Code |
| Fixtures/testes do contrato | trocar respostas simuladas da Responses API por saídas JSON simuladas do `claude -p`; incluir casos de `is_error`, timeout, código de saída ≠ 0 e JSON fora do schema |
| Nome do plano | renomeado para `2026-08-12-statistical-analyst-claude-implementation-plan.md` |

## 7. Dados obrigatórios de implantação (novos ou alterados)

- provedor e plano da VPS, IP e acesso SSH por chave;
- confirmação de saída liberada para o broker e para o Gmail SMTP;
- hostname, porta e CA do broker público;
- forma de autenticação do Claude (chave de API ou assinatura) e responsável por ela;
- limite de gasto mensal (se for chave de API);
- destino externo do backup cifrado;
- decisão sobre o *heartbeat* externo (seção 5).

Nenhum desses valores secretos é versionado.

## 8. Critérios de aceitação adicionais

1. Com a VPS reiniciada, `mqtt-core.service` volta sozinho e retoma a assinatura sem intervenção.
2. Matar o processo do Core gera reinício automático pelo systemd em menos de 30 s, sem alerta duplicado.
3. Travar o laço principal (teste com injeção) faz o watchdog reiniciar o serviço.
4. O usuário `agente-analista` não consegue escrever em `operational.sqlite` (teste de permissão).
5. Uma execução do `claude -p` com o pacote de teste não consegue usar nenhuma ferramenta (teste com prompt que pede leitura de arquivo).
6. Sem credencial do Claude, ou com a cota esgotada, o relatório sai em fallback local e os alarmes continuam.
7. Restauração do backup externo reproduz o banco íntegro (`PRAGMA integrity_check`).
8. Todos os critérios das especificações originais continuam aprovados no ambiente VPS.
