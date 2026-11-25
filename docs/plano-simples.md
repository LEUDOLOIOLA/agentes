# Plano simples — Monitor MQTT de reservatórios + Analista estatístico

**Onde roda:** VPS Hostinger, Ubuntu 24.04, Python 3.12, serviços `systemd`.
**Ordem:** primeiro o Monitor (Tasks M1–M6) funcionando; só depois o Analista (Tasks A1–A5).
**Regra geral:** código pequeno, configuração em `.env` (nunca no Git), testes básicos com `pytest`, e um commit por task.

## Parte 1 — Monitor MQTT

O monitor assina `EPZ/META/0051/ANI01/RAW` e `EPZ/META/0022/ANI01/RAW`. O payload é um número de 0 a 4096, e pressão em bar = valor / 1024. Ele envia e-mail pelo Gmail em três casos:

- pressão ≤ 1,0 bar (risco de secar);
- pressão ≥ 1,2 bar (risco de extravasar);
- mais de 30 min sem leitura.

O mesmo alerta não se repete mais de uma vez por hora, e há aviso de normalização. Sem IA.

Estrutura:
```text
monitor/
  config.py      # lê o .env
  regras.py      # conversão, validação e decisão de alertas (funções puras)
  banco.py       # SQLite: leituras e alertas
  email_gmail.py # envio SMTP
  main.py        # MQTT + loop de verificação
tests/
.env.example
deploy/monitor.service
README.md
```

### Task M1 — Estrutura e configuração
- Criar `requirements.txt` (`paho-mqtt`, `python-dotenv`, `pytest`), `.env.example` e `monitor/config.py`.
- O `.env` contém: host, porta, usuário e senha do MQTT; usuário Gmail; senha de app do Gmail; destinatários; caminho do banco.
- **Pronto quando:** `config.py` carrega o `.env` e falha com mensagem clara se faltar algum campo (com teste).

### Task M2 — Regras (sem rede)
Em `monitor/regras.py`:
- `converter(payload) -> bar | None`: aceita só número de 0 a 4096; texto inválido devolve `None`.
- `classificar(bar) -> "SECAR" | "EXTRAVASAR" | "NORMAL"`, com limites ≤ 1,0 e ≥ 1,2.
- `deve_alertar(tipo, ultimo_envio, agora)`: no máximo um alerta igual por hora.
- **Pronto quando:** os testes cobrem 0, 1024, 1228, 1229, 4096, valores fora da faixa, texto inválido e a regra de 1 hora.

### Task M3 — Banco SQLite
- `monitor/banco.py` com as tabelas `leituras(topico, valor_bruto, bar, recebido_em)` e `alertas(reservatorio, tipo, enviado_em, resolvido_em)`.
- Funções: gravar leitura, última leitura por tópico, registrar alerta e marcar como resolvido.
- **Pronto quando:** os testes com banco temporário passam.

### Task M4 — E-mail
- `monitor/email_gmail.py`: envio por `smtp.gmail.com:587` com STARTTLS e senha de app.
- O e-mail traz reservatório, tipo, valor bruto, bar e horário.
- Se o envio falhar, registra no log e tenta de novo no próximo ciclo, sem derrubar o programa.
- **Pronto quando:** o teste com SMTP falso passa e um envio manual de teste (`python -m monitor.email_gmail --teste`) chega na caixa.

### Task M5 — Programa principal
- `monitor/main.py`: conecta no MQTT com TLS e reconexão automática, assina os 2 tópicos e grava cada leitura.
- Na chegada de cada leitura, verifica SECAR/EXTRAVASAR e alerta na hora.
- A cada 1 minuto, verifica os tópicos sem leitura há mais de 30 min.
- Quando volta ao normal, envia "normalizado" e marca o alerta como resolvido.
- **Pronto quando:** rodando localmente contra o broker, as leituras aparecem no banco e um valor de teste dispara o e-mail.

### Task M6 — Instalação na Hostinger
- `deploy/monitor.service` com `systemd`, `Restart=always` e usuário próprio sem login.
- `README.md` passo a passo:
  1. criar a VPS Ubuntu 24.04 no hPanel com chave SSH;
  2. `apt install python3.12-venv git`;
  3. firewall liberando só SSH;
  4. `git clone`, venv e `pip install -r requirements.txt`;
  5. copiar `.env.example` para `.env` (`chmod 600`);
  6. instalar e iniciar o serviço;
  7. ver os logs com `journalctl -u monitor -f`.
- Antes de tudo, testar se a VPS alcança o broker e o `smtp.gmail.com:587`.
- **Pronto quando:** o serviço sobe sozinho após `sudo reboot` e os alertas chegam.

## Parte 2 — Analista estatístico (depois do monitor funcionando)

A cada 4 horas (00h, 04h, 08h, 12h, 16h e 20h, horário de Fortaleza), o analista lê o SQLite do monitor **só para leitura** e calcula as estatísticas em Python. Em seguida, pede ao Claude Code (`claude -p`, sem ferramentas) um resumo curto e envia um e-mail consultivo. Se o Claude falhar, o e-mail vai só com os números. O analista não interfere no monitor.

Estrutura:
```text
analista/
  estatistica.py # cálculos (funções puras)
  leitura.py     # consulta read-only ao SQLite do monitor
  claude.py      # chamada ao claude -p com timeout
  relatorio.py   # monta e envia o e-mail (reusa monitor/email_gmail.py)
  main.py        # executa uma análise e termina
deploy/analista.service
deploy/analista.timer
```

### Task A1 — Cálculos
- `analista/estatistica.py`: para cada reservatório, calcula mínimo, máximo, média, última leitura, tendência (subindo, descendo ou estável, pela diferença entre o início e o fim da janela, com margem de 0,01 bar), leituras recebidas e leituras esperadas (48 em 4 h).
- Também compara com a média das 24 h anteriores.
- **Pronto quando:** os testes com séries conhecidas passam, inclusive a série vazia.

### Task A2 — Leitura do banco
- `analista/leitura.py` abre o SQLite com `mode=ro` e busca as leituras da janela por tópico.
- **Pronto quando:** o teste confirma que o `UPDATE` falha nessa conexão.

### Task A3 — Chamada ao Claude
- `analista/claude.py` executa `claude -p --output-format json --tools ""` com timeout de 60 s, mandando o JSON dos números pela entrada padrão.
- O prompt pede um resumo em português de no máximo 5 linhas, sem inventar números e sem comandar equipamentos.
- Em erro ou timeout, devolve `None`.
- Credencial: `ANTHROPIC_API_KEY` (recomendado) ou login da assinatura, configurada no `.env`.
- **Pronto quando:** o teste com um `claude` falso cobre sucesso, erro e timeout.

### Task A4 — Relatório por e-mail
- `analista/relatorio.py` monta o e-mail com o título "Análise consultiva por IA — não é comando operacional": uma tabela com os números dos 2 reservatórios e, abaixo, o resumo do Claude, ou o aviso "resumo indisponível".
- Reutiliza o envio do monitor.
- **Pronto quando:** o teste do texto gerado passa, nos casos com e sem resumo.

### Task A5 — Agendamento na Hostinger
- `analista/main.py` faz uma execução e termina.
- `deploy/analista.timer` com `OnCalendar=*-*-* 00/4:02:00 America/Fortaleza` e `Persistent=true`, mais `analista.service` (`Type=oneshot`).
- O README ganha a seção "Instalar o Claude Code na VPS e configurar a credencial".
- **Pronto quando:** `systemctl list-timers` mostra o próximo horário e uma execução manual (`systemctl start analista`) entrega o e-mail.
