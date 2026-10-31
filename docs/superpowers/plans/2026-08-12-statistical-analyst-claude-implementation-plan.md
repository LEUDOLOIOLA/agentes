# Statistical Analyst Claude Implementation Plan

> **Revisão 2026-10-31:** plano reescrito para VPS Linux com `systemd` e para Claude Code headless (`claude -p`) no lugar da OpenAI Responses API, conforme `../specs/2026-10-31-migracao-vps-claude-code-design.md`. Estatística, selagem, validação, fallback e relatório seguem a especificação de 2026-08-10, trocando o provedor.


> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir um segundo serviço `systemd`, na mesma VPS Linux do Core, que a cada 4 horas calcula estatísticas locais independentes dos reservatórios `0051` e `0022`, solicita interpretação estruturada ao modelo Claude fixo via Claude Code headless e entrega um único relatório consultivo pelo Gmail com fallback determinístico.

**Architecture:** `statistical_analyst` abre `operational.sqlite` somente para leitura e captura um snapshot consistente sem compartilhar workers, locks ou outbox com o MQTT Core. Métricas, suficiência, prioridade e alarmes são locais; dois subprocessos `claude -p` independentes, sem ferramentas, interpretam pacotes selados, e um validador estrito rejeita qualquer saída incompleta, sem evidência ou com linguagem de controle. Estado analítico, orçamento, circuit breaker, relatórios e outbox residem em `analytics.sqlite` próprio.

**Tech Stack:** CPython 3.12, biblioteca padrão (`asyncio.subprocess`), Claude Code CLI em versão fixada (instalado em `/opt/claude-code`, autoatualização desligada), `pydantic==2.13.4`, `tzdata==2026.3`, `pytest==9.1.1`, `pytest-cov==7.1.0`, `pytest-asyncio==1.4.0`.

## Global Constraints

- Pré-requisito: concluir o plano `2026-08-12-mqtt-core-implementation-plan.md`; este serviço consome o contrato SQL criado ali e nunca migra `operational.sqlite`.
- Processo, usuário Linux (`agente-analista`), unidade `systemd`, prioridade (`Nice=10`, `IOSchedulingClass=idle`, `CPUWeight=20`), banco, writer, outbox, dispatcher Gmail e logs são distintos do MQTT Core.
- O serviço é contínuo (`Type=notify`): o agendador interno de 4 horas e o dispatcher Gmail com retry de até 60 minutos continuam no processo. Não se usa timer oneshot, porque ele interromperia retries pendentes.
- Modelo fixo `claude-sonnet-5-5` via `claude -p --model claude-sonnet-5-5 --effort low --output-format json --json-schema <schema> --system-prompt-file <prompt> --tools "" --strict-mcp-config --setting-sources "" --disable-slash-commands --no-session-persistence --max-budget-usd <limite>`. Proibidos: `--fallback-model`, `--resume`/`--continue`, `--mcp-config`, `--add-dir`, `--allowedTools`, `--dangerously-skip-permissions`.
- Subprocesso com CWD vazio (`/var/lib/agente-analista/claude-empty`), `HOME` dedicado, ambiente mínimo (`PATH`, `HOME`, credencial, `DISABLE_AUTOUPDATER=1`, `CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1`, `CLAUDE_CODE_MAX_OUTPUT_TOKENS=4000`), pacote enviado por `stdin`, grupo de processos próprio e morto em timeout.
- Versão do CLI fixada em configuração e conferida por `claude --version` no preflight; divergência impede chamadas e gera fallback até nova homologação.
- Horários: `00:00`, `04:00`, `08:00`, `12:00`, `16:00`, `20:00` em `America/Fortaleza`; snapshot começa em `window_end + 2 minutos`.
- Uma execução analítica global ativa; deadline total de 3 minutos, `busy_timeout=2s`, leitura operacional total de no máximo 10 segundos.
- Um pacote selado por reservatório e janela; bytes canônicos, hash e manifesto persistidos antes de qualquer subprocesso Claude.
- Janela atual `[end-4h,end)`, referência não sobreposta `[end-28h,end-4h)` e sete janelas de 4 horas no mesmo horário local dos dias anteriores.
- Estatísticas, tendência, ETA, variabilidade, status, confiança, prioridade, alarmes e números do e-mail são calculados localmente.
- Duas chamadas independentes, concorrência máxima 2; falha de uma afeta somente a seção correspondente.
- Payload completo (prompt de sistema + pacote + schema) máximo 16.000 bytes UTF-8, e fixtures verificadas em até 5.000 tokens de entrada **efetivos** medidos pelo `usage` do próprio `claude -p`, somando `input_tokens`, `cache_creation_input_tokens` e `cache_read_input_tokens`. Saída limitada por `CLAUDE_CODE_MAX_OUTPUT_TOKENS=4000`; JSON estruturado máximo 6.000 bytes.
- Máximo 24 tentativas, 120.000 tokens de entrada reservados, 96.000 de saída e o orçamento diário em USD (`daily_budget_usd`) em qualquer janela móvel de 24 horas. `--max-budget-usd` por chamada é uma segunda barreira, não a principal.
- Uma repetição por reservatório somente para falhas transitórias classificadas por allowlist versionada: timeout do subprocesso, erro de rede, sobrecarga/5xx/529 e rate limit temporário. Cada tentativa física exige nova reserva.
- `is_error=true`, `subtype` diferente de `success`, ausência de `structured_output`, uso de qualquer ferramenta, modelo diferente do fixo em `modelUsage`, erro semântico, schema inválido ou conteúdo inesperado sempre geram fallback e nunca retry semântico.
- Narrativa livre não pode conter algarismos; action codes vêm de catálogo fechado; a LLM nunca escolhe urgência, destinatários, assunto ou cadência.
- Retenção analítica de 90 dias. Segredos só via `systemd` `LoadCredential=` (`claude.credential`, `gmail.username`, `gmail.app_password`), nunca em TOML, Git, argumento de linha de comando ou log. Logs guardam hashes, métricas, `session_id` e custo, nunca prompts, respostas, e-mails, `stderr` bruto do CLI ou credenciais.
- Credencial do Claude: `ANTHROPIC_API_KEY` de workspace dedicado (produção recomendada) ou `CLAUDE_CODE_OAUTH_TOKEN` de `claude setup-token` (piloto, se os termos do plano permitirem). O tipo é declarado em `claude_credential_kind` e vira a única variável de credencial passada ao subprocesso.
- Contrato, prompt e catálogo são instalados em diretório absoluto versionado (`/opt/agente-mqtt/resources/statistical-analyst`); nenhum loader depende do current working directory.
- Cada tarefa segue TDD e termina em commit próprio.

## Mapa de arquivos

```text
pyproject.toml
config/statistical-analyst.example.toml
resources/
  contracts/operational_v1.json  # criado pelo plano Core; somente consumido aqui
  prompts/statistical_analyst_v1.txt
  catalogs/action_codes_v1.json
deploy/systemd/
  statistical-analyst.service
scripts/
  install-statistical-analyst.sh
  uninstall-statistical-analyst.sh
  verify-statistical-analyst.sh
src/statistical_analyst/
  __init__.py
  __main__.py
  app.py
  clock.py
  config.py
  ports.py
  scheduler.py
  security.py
  observability.py
  domain/
    operational.py
    analysis.py
    claude_contract.py
    report.py
  persistence/
    migrations/0001_analytics.sql
    analytics_db.py
    analysis_store.py
    usage_store.py
    outbox_store.py
    operational_reader.py
  analysis/
    windows.py
    slot_selection.py
    sufficiency.py
    descriptive.py
    trends.py
    historical.py
    state_durations.py
    classification.py
    package_builder.py
  ai/
    prompt.py
    request_builder.py
    response_validator.py
    retry_policy.py
    circuit_breaker.py
    claude_client.py
    cli_envelope.py
  reporting/
    fallback.py
    renderer.py
    gmail_transport.py
    dispatcher.py
  pilot/
    __init__.py
    evaluator.py
  service/
    runtime.py
    linux_service.py
tests/
  unit/
  integration/
  e2e/
  operational/
  live/
  fixtures/golden/
```

## Interfaces estáveis entre tarefas

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

ReservoirId = Literal["0051", "0022"]

@dataclass(frozen=True)
class WindowKey:
    window_end_utc: datetime
    analysis_version: str

class OperationalSnapshotReader(Protocol):
    def capture(self, key: WindowKey, *, deadline_monotonic: float) -> "OperationalSnapshot": ...

class AnalysisStore(Protocol):
    def get_or_create_batch(self, key: WindowKey) -> "AnalysisBatch": ...
    def seal_input(self, batch_id: UUID, reservoir_id: ReservoirId,
                   package: "AnalysisPackage", manifest: "SnapshotManifest") -> "SealedInput": ...
    def load_sealed_input(self, batch_id: UUID, reservoir_id: ReservoirId) -> "SealedInput | None": ...

class UsageBudget(Protocol):
    def try_reserve(self, attempt_id: UUID, now: datetime,
                    input_tokens: int = 5000, output_tokens: int = 4000) -> "UsageReservation | None": ...
    def reconcile(self, reservation_id: UUID, usage: "TokenUsage | None", now: datetime) -> None: ...

class SecretProvider(Protocol):
    def get_required(self, target_name: str) -> str: ...

class ClaudeTransport(Protocol):
    async def analyze(self, prepared: "PreparedClaudeRequest", client_request_id: UUID) -> "CliEnvelope": ...

class ResponseValidator(Protocol):
    def validate(self, envelope: "CliEnvelope", sealed: "SealedInput") -> "ValidatedInterpretation | ValidationFailure": ...

class ReportOutbox(Protocol):
    def enqueue(self, report: "RenderedReport") -> None: ...
    def claim_due(self, now: datetime, worker_id: UUID) -> "OutboxMessage | None": ...
    def mark_sent(self, outbox_id: UUID, provider_id: str, sent_at: datetime) -> None: ...
    def reschedule(self, outbox_id: UUID, next_attempt_at: datetime, error_code: str) -> None: ...
```

---

### Task 1: Dependências analíticas, configuração e DTOs-base

**Files:**
- Modify: `pyproject.toml`
- Modify: `requirements/production.lock`
- Modify: `requirements/test.lock`
- Create: `config/statistical-analyst.example.toml`
- Create: `src/statistical_analyst/__init__.py`
- Create: `src/statistical_analyst/clock.py`
- Create: `src/statistical_analyst/config.py`
- Create: `src/statistical_analyst/secrets.py`
- Create: `src/statistical_analyst/ports.py`
- Create: `src/statistical_analyst/ai/__init__.py`
- Create: `src/statistical_analyst/analysis/__init__.py`
- Create: `src/statistical_analyst/domain/__init__.py`
- Create: `src/statistical_analyst/persistence/__init__.py`
- Create: `src/statistical_analyst/reporting/__init__.py`
- Create: `src/statistical_analyst/service/__init__.py`
- Create: `src/statistical_analyst/domain/operational.py`
- Create: `src/statistical_analyst/domain/analysis.py`
- Create: `tests/unit/test_analyst_config.py`
- Create: `tests/unit/test_analyst_secrets.py`
- Create: `tests/unit/test_analysis_models.py`

**Interfaces:**
- Consumes: caminhos e contrato SQL produzidos pelo MQTT Core.
- Produces: `AnalystConfig`, `WindowKey`, `OperationalEvent`, `OperationalMeasurement`, `StateTransition`, `OperationalSnapshot`, `SnapshotManifest` e Protocols do mapa acima.

- [ ] **Step 1: Escrever testes de separação de caminhos e UTC aware**

```python
# tests/unit/test_analyst_config.py
def test_config_requires_distinct_absolute_databases(tmp_path) -> None:
    cfg = load_analyst_config(write_valid_config(tmp_path))
    assert cfg.operational_database.is_absolute()
    assert cfg.analytics_database.is_absolute()
    assert cfg.resource_directory.is_absolute()
    assert cfg.operational_database != cfg.analytics_database
    assert cfg.model == "claude-sonnet-5-5"
    assert cfg.claude_credential_name == "claude.credential"
    assert cfg.claude_credential_kind in {"api_key", "oauth_token"}
    assert cfg.claude_cli_path.is_absolute()

def test_config_rejects_fallback_model_and_unpinned_cli(tmp_path) -> None:
    with pytest.raises(ConfigError, match="fallback"):
        load_analyst_config(write_config(tmp_path, fallback_model="claude-haiku-5-5"))
    with pytest.raises(ConfigError, match="claude_cli_version"):
        load_analyst_config(write_config(tmp_path, claude_cli_version="latest"))

def test_config_rejects_recipient_inside_claude_payload_fields(tmp_path) -> None:
    path = write_config(tmp_path, payload_fields=["reservoir_id", "gmail_recipients"])
    with pytest.raises(ConfigError, match="allowlist"):
        load_analyst_config(path)

# tests/unit/test_analysis_models.py
def test_window_key_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        WindowKey(datetime(2026, 8, 12), "analysis-v1")

# tests/unit/test_analyst_secrets.py
def test_systemd_provider_reads_current_file_on_every_request(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))
    provider = SystemdCredentialProvider()
    (tmp_path / "claude.credential").write_text("first\n")
    assert provider.get_required("claude.credential") == "first"
    (tmp_path / "claude.credential").write_text("rotated\n")
    assert provider.get_required("claude.credential") == "rotated"

@pytest.mark.parametrize("name", ["../etc/passwd", "a/b", ""])
def test_systemd_provider_rejects_unsafe_names(tmp_path, monkeypatch, name) -> None:
    monkeypatch.setenv("CREDENTIALS_DIRECTORY", str(tmp_path))
    with pytest.raises(ConfigError):
        SystemdCredentialProvider().get_required(name)
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_analyst_config.py tests/unit/test_analyst_secrets.py tests/unit/test_analysis_models.py -v`

Expected: FAIL porque o pacote não existe.

- [ ] **Step 3: Adicionar pins e modelos mínimos**

```toml
# modificar as tabelas existentes em pyproject.toml; não redeclarar cabeçalhos TOML
[project.optional-dependencies]
analytics = ["pydantic==2.13.4"]
test = ["pytest==9.1.1", "pytest-cov==7.1.0", "pytest-asyncio==1.4.0"]

[tool.pytest.ini_options]
asyncio_mode = "strict"
markers = ["live: calls a paid external API", "operational: requires root on a systemd host", "mosquitto: requires local Mosquitto", "soak: long-running operational test"]
```

Ao editar, preservar `testpaths` e `addopts` criados pelo Core, adicionar `pytest-asyncio` à lista `test` existente e criar a chave `analytics`; um teste de parsing com `tomllib` deve assegurar que o arquivo final contém uma única tabela de cada nome.

Atualizar os lockfiles existentes com hashes de todas as dependências transitivas e reinstalar no mesmo ambiente isolado criado pelo plano Core:

```bash
.venv/bin/python -m pip install --require-hashes -r requirements/production.lock
.venv/bin/python -m pip install --require-hashes -r requirements/test.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps -e .
.venv/bin/python -m pip check
```

```python
# src/statistical_analyst/domain/analysis.py
@dataclass(frozen=True)
class WindowKey:
    window_end_utc: datetime
    analysis_version: str
    def __post_init__(self) -> None:
        if self.window_end_utc.tzinfo is None or self.window_end_utc.utcoffset() is None:
            raise ValueError("window_end_utc must be timezone-aware")

ReservoirId: TypeAlias = Literal["0051", "0022"]
```

O TOML deve conter caminhos absolutos separados (`operational_database=/var/lib/agente-mqtt/operational.sqlite`, `analytics_database=/var/lib/agente-analista/analytics.sqlite`, `resource_directory=/opt/agente-mqtt/resources/statistical-analyst`, `claude_cli_path=/opt/claude-code/bin/claude`, `claude_workdir=/var/lib/agente-analista/claude-empty`, `claude_home=/var/lib/agente-analista/claude-home`), `claude_cli_version` exata, `claude_credential_kind`, `max_budget_usd_per_call`, `daily_budget_usd`, timezone, modelo fixo, horários, snapshot delay, deadlines, limites, Gmail e nomes das credenciais `systemd` (`claude_credential_name` e `gmail_credential_name`); nenhum segredo. Rejeitar chaves extras, modelo divergente, banco inexistente no preflight e diretórios graváveis compartilhados com o Core, exceto leitura do arquivo operacional. Rejeitar também `fallback_model`, `claude_cli_version` sem versão exata e `claude_workdir` não vazio ou dentro de um repositório. `SystemdCredentialProvider` lê `$CREDENTIALS_DIRECTORY/<nome>` em toda solicitação, remove só o `\n` final, rejeita nome com `/` ou `..`, link simbólico e arquivo vazio, devolve erro seguro sem conteúdo e nunca faz cache/log do valor. Ele é reutilizado pelo cliente Claude e pelo Gmail. Rotação: atualizar `/etc/credstore/agente-analista/<nome>` e `systemctl restart statistical-analyst`, porque o `systemd` só recopia credenciais na partida; a leitura sem cache garante que nenhum valor antigo sobreviva no processo.

- [ ] **Step 4: Executar testes e checar import das versões fixadas**

Run: `.venv/bin/python -m pytest tests/unit/test_analyst_config.py tests/unit/test_analyst_secrets.py tests/unit/test_analysis_models.py -v`

Expected: PASS.

Run: `.venv/bin/python -c "import pydantic; assert pydantic.__version__=='2.13.4'"`

Expected: exit code 0 no ambiente instalado pelos locks.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml requirements config/statistical-analyst.example.toml src/statistical_analyst tests/unit/test_analyst_config.py tests/unit/test_analyst_secrets.py tests/unit/test_analysis_models.py
git commit -m "build: scaffold statistical analyst service"
```

### Task 2: Banco analítico, batches e migração idempotente

**Files:**
- Modify: `pyproject.toml`
- Create: `src/statistical_analyst/persistence/migrations/0001_analytics.sql`
- Create: `src/statistical_analyst/persistence/analytics_db.py`
- Create: `src/statistical_analyst/persistence/analysis_store.py`
- Create: `src/statistical_analyst/persistence/usage_store.py`
- Create: `src/statistical_analyst/persistence/outbox_store.py`
- Create: `tests/integration/test_analytics_schema.py`
- Create: `tests/integration/test_analysis_store.py`
- Create: `tests/integration/test_analyst_wheel_artifact.py`

**Interfaces:**
- Consumes: `WindowKey`, `ReservoirId`.
- Produces: `analysis_batches UNIQUE(window_end_utc,analysis_version)`, duas `analysis_runs` por batch, stores transacionais e `migrate_analytics(path)`.

- [ ] **Step 1: Escrever testes do schema e idempotência global**

```python
# tests/integration/test_analytics_schema.py
EXPECTED = {
    "analysis_batches", "analysis_runs", "analysis_inputs", "analysis_responses",
    "analysis_usage", "analysis_reports", "analysis_outbox", "usage_reservations",
    "circuit_breaker_state", "schema_migrations",
}

def test_analytics_migration_is_reentrant(tmp_path) -> None:
    path = tmp_path / "analytics.sqlite"
    migrate_analytics(path); migrate_analytics(path)
    with open_analytics(path) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert EXPECTED <= tables

def test_batch_and_reservoir_runs_are_idempotent(analysis_store, window_key) -> None:
    first = analysis_store.get_or_create_batch(window_key)
    second = analysis_store.get_or_create_batch(window_key)
    assert first.batch_id == second.batch_id
    assert {run.reservoir_id for run in analysis_store.list_runs(first.batch_id)} == {"0051", "0022"}

# tests/integration/test_analyst_wheel_artifact.py
def test_isolated_wheel_can_create_analytics_database(isolated_wheel_install) -> None:
    probe = isolated_wheel_install.run_resource_probe(cwd=isolated_wheel_install.empty_directory)
    assert probe.package_file("statistical_analyst.persistence", "migrations/0001_analytics.sql").exists
    assert probe.migrate_fresh_analytics_database().schema_version == 1
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/integration/test_analytics_schema.py tests/integration/test_analysis_store.py tests/integration/test_analyst_wheel_artifact.py -v`

Expected: FAIL por persistência ausente.

- [ ] **Step 3: Implementar schema separado e constraints**

Adicionar à tabela `[tool.setuptools.package-data]` já criada pelo Core, sem redeclarar o cabeçalho:

```toml
"statistical_analyst.persistence" = ["migrations/*.sql"]
```

A mesma fixture de wheel isolado do Core constrói e instala o artefato sem checkout/editable antes de executar a migration analítica. `migrate_analytics()` lê o SQL via `importlib.resources.files("statistical_analyst.persistence").joinpath("migrations/0001_analytics.sql")`, nunca por CWD.

```sql
CREATE TABLE analysis_batches (
  batch_id TEXT PRIMARY KEY,
  window_end_utc TEXT NOT NULL,
  analysis_version TEXT NOT NULL,
  status TEXT NOT NULL,
  snapshot_at_utc TEXT,
  created_at_utc TEXT NOT NULL,
  completed_at_utc TEXT,
  UNIQUE(window_end_utc, analysis_version)
);

CREATE TABLE analysis_runs (
  run_id TEXT PRIMARY KEY,
  batch_id TEXT NOT NULL REFERENCES analysis_batches(batch_id),
  reservoir_id TEXT NOT NULL CHECK (reservoir_id IN ('0051','0022')),
  status TEXT NOT NULL,
  fallback_reason TEXT,
  UNIQUE(batch_id, reservoir_id)
);

CREATE TABLE analysis_inputs (
  input_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL UNIQUE REFERENCES analysis_runs(run_id),
  canonical_json BLOB NOT NULL,
  package_sha256 TEXT NOT NULL,
  snapshot_manifest_json BLOB NOT NULL,
  package_bytes INTEGER NOT NULL CHECK (package_bytes <= 16000),
  sealed_at_utc TEXT NOT NULL
);

CREATE TABLE usage_reservations (
  reservation_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL UNIQUE,
  reserved_at_utc TEXT NOT NULL,
  reserved_input_tokens INTEGER NOT NULL CHECK (reserved_input_tokens >= 0),
  reserved_output_tokens INTEGER NOT NULL CHECK (reserved_output_tokens >= 0),
  accounted_input_tokens INTEGER NOT NULL CHECK (accounted_input_tokens >= 0),
  accounted_output_tokens INTEGER NOT NULL CHECK (accounted_output_tokens >= 0),
  usage_known INTEGER NOT NULL CHECK (usage_known IN (0,1))
);
```

Adicionar respostas, usage (incluindo `full_request_bytes`), report global por batch, outbox global com fingerprint UNIQUE, reservations append-only e breaker com uma linha por scope. `package_bytes` mede apenas o JSON selado; o limite autoritativo de 16.000 para prompt+JSON+schema é aplicado pelo request builder da Task 14. Em `usage_reservations`, `accounted_*` inicia igual à reserva; reconciliação com usage confiável substitui pelos valores reais, enquanto ausência de usage mantém a reserva integral. Os limites móveis somam `accounted_input_tokens` e `accounted_output_tokens`; nunca subtraem tentativas sem usage. Todos os timestamps analíticos usam também `YYYY-MM-DDTHH:MM:SS.ffffffZ`, com serializer/testes de round-trip e borda exata de 24 horas. Toda conexão: WAL, FULL, FKs, busy timeout 5s e transações explícitas. Não reutilizar `SerializedDatabaseWriter` do Core em processo; implementar writer próprio no pacote analítico.

- [ ] **Step 4: Executar testes de migração, concorrência e rollback**

Run: `.venv/bin/python -m pytest tests/integration/test_analytics_schema.py tests/integration/test_analysis_store.py tests/integration/test_analyst_wheel_artifact.py -v`

Expected: PASS, inclusive dois criadores concorrentes obtendo o mesmo batch e wheel isolado capaz de migrar sem source tree.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml src/statistical_analyst/persistence tests/integration/test_analytics_schema.py tests/integration/test_analysis_store.py tests/integration/test_analyst_wheel_artifact.py
git commit -m "feat: persist isolated analytics execution state"
```

### Task 3: Leitor read-only e snapshot operacional consistente

**Files:**
- Consume installed copy: `<resource_directory>/contracts/operational_v1.json`
- Create: `src/statistical_analyst/persistence/operational_reader.py`
- Create: `tests/integration/test_operational_snapshot.py`
- Create: `tests/integration/test_operational_query_deadline.py`

**Interfaces:**
- Consumes: schema `operational.sqlite` congelado no plano do Core.
- Produces: `OperationalSnapshotReader.capture(key, deadline_monotonic) -> OperationalSnapshot` e `SnapshotManifest`.

- [ ] **Step 1: Escrever testes de query-only, consistência e deadline**

```python
# tests/integration/test_operational_snapshot.py
def test_reader_cannot_modify_operational_database(operational_reader) -> None:
    with pytest.raises(sqlite3.OperationalError, match="readonly|read-only"):
        operational_reader.execute_for_test("UPDATE runtime_health SET mqtt_connected=0")

def test_snapshot_does_not_see_commit_after_transaction_started(core_writer, operational_reader, window_key) -> None:
    barrier = SnapshotBarrier()
    future = barrier.capture_async(operational_reader, window_key)
    barrier.wait_until_snapshot_pinned_by_first_select()
    core_writer.insert_measurement(event_id="late", received_at=window_key.window_end_utc - timedelta(minutes=1))
    barrier.release()
    snapshot = future.result()
    assert "late" not in snapshot.manifest.event_ids

def test_manifest_is_deterministic(operational_reader, window_key) -> None:
    first = operational_reader.capture(window_key, deadline_monotonic=999999)
    second = operational_reader.capture(window_key, deadline_monotonic=999999)
    assert first.manifest == second.manifest

def test_snapshot_reads_exact_core_schema_contract(operational_reader, window_key) -> None:
    snapshot = operational_reader.capture(window_key, deadline_monotonic=999999)
    assert all(event.measurement is None or event.measurement.event_id == event.event_id for event in snapshot.events)
    assert {(alarm.alarm_id, alarm.lifecycle) for alarm in snapshot.alarm_events}
    assert snapshot.active_alarms == derive_active_alarms(snapshot.alarm_events)

def test_reader_rejects_unknown_or_not_ready_operational_contract(operational_db, window_key) -> None:
    operational_db.set_contract(contract_version="operational-v2", ready=1)
    with pytest.raises(UnsupportedOperationalContract):
        make_reader(operational_db.path).capture(window_key, deadline_monotonic=999999)
    operational_db.set_contract(contract_version="operational-v1", ready=0)
    with pytest.raises(OperationalCoreNotReady):
        make_reader(operational_db.path).capture(window_key, deadline_monotonic=999999)

def test_reader_includes_left_carry_for_each_machine(operational_db, window_key) -> None:
    start = window_key.window_end_utc - timedelta(hours=4)
    operational_db.insert_transition("0051", "quality", "GOOD", start - timedelta(seconds=1))
    operational_db.insert_transition("0051", "process", "NORMAL", start - timedelta(seconds=2))
    operational_db.insert_transition("0051", "quality", "MISSING", start + timedelta(minutes=15))
    snapshot = make_reader(operational_db.path).capture(window_key, deadline_monotonic=999999)
    assert snapshot.transitions_for("0051", "quality")[0].effective_at_utc < start
    assert snapshot.transitions_for("0051", "process")[0].effective_at_utc < start

def test_reader_uses_canonical_half_open_timestamp_bounds(operational_db, window_key) -> None:
    start = window_key.window_end_utc - timedelta(hours=4)
    operational_db.insert_measurements_at([start, window_key.window_end_utc - timedelta(microseconds=1), window_key.window_end_utc])
    snapshot = make_reader(operational_db.path).capture(window_key, deadline_monotonic=999999)
    assert snapshot.current_measurement_times == [start, window_key.window_end_utc - timedelta(microseconds=1)]
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/integration/test_operational_snapshot.py tests/integration/test_operational_query_deadline.py -v`

Expected: FAIL por reader ausente.

- [ ] **Step 3: Implementar URI read-only, transação e progress handler**

```python
# src/statistical_analyst/persistence/operational_reader.py
def _connect_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, isolation_level=None, timeout=2.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=2000")
    return conn

def capture(self, key: WindowKey, *, deadline_monotonic: float) -> OperationalSnapshot:
    with _connect_readonly(self._path) as conn:
        conn.set_progress_handler(lambda: 1 if self._clock.monotonic() >= deadline_monotonic else 0, 1000)
        conn.execute("BEGIN")
        # BEGIN é deferred; esta leitura real fixa o snapshot WAL antes da barreira de concorrência.
        contract_row = conn.execute(
            "SELECT schema_version,contract_version,rule_version,timestamp_format,"
            "manifest_sha256,activated_at_utc,ready FROM operational_contract WHERE singleton_id=1"
        ).fetchone()
        self._validate_contract(contract_row, load_operational_contract_v1())
        snapshot_at = self._clock.utc_now()
        rows = self._read_all_windows(conn, key)
        manifest = build_manifest(snapshot_at, rows)
        conn.execute("COMMIT")
    return OperationalSnapshot.from_rows(rows, manifest)
```

Antes de ler dados, comparar versão, `ready=1`, `timestamp_format`, `rule_version`, SHA-256 e a matriz exata de colunas obtida por `PRAGMA table_info` com o caminho absoluto `<resource_directory>/contracts/operational_v1.json` da configuração. O teste muda o CWD para um diretório vazio e ainda captura o snapshot. O Analyst implementa seu próprio parser estrito para `YYYY-MM-DDTHH:MM:SS.ffffffZ`; não importa serializer, store nem migration do Core. Divergência vira fallback de snapshot e zero HTTP.

As consultas devem selecionar explicitamente as colunas do manifesto, nunca `SELECT *`; usar bounds formatados canonicamente e intervalos semiabertos por `received_at_utc`; ler `raw_events` com `LEFT JOIN measurements` por `event_id`, incluindo separadamente o bit bruto `duplicate` e `delivery_disposition`, além de `state_transitions`, o log append-only `alarms`, `runtime_health` e `operational_contract`; ordenar por timestamps e IDs. O DTO rejeita disposição fora de `NEW/FIRST_LOCAL_COPY/ALREADY_COMMITTED`. Para cada intervalo e cada par `(reservoir_id,machine)`, executar uma query para a última transição com `effective_at_utc < start` e uni-la às transições `effective_at_utc >= start AND effective_at_utc < end`; deduplicar por `transition_id`. Isso fornece o left carry separadamente para qualidade e processo nas janelas atual, 24h e sete dias. Alarmes ativos são derivados pela última lifecycle de cada `alarm_id`, sendo `RECOVERED` inativo e `STATUS_UPDATED` ainda ativo. O deadline total passado pelo caller é `start+10s`.

Converter `sqlite3.OperationalError` causado pelo progress handler em `SnapshotDeadlineExceeded`, fazer rollback e fechar a conexão. O snapshot guarda `contract_version`, `rule_version` e `manifest_sha256`; esses valores entram no hash selado.

- [ ] **Step 4: Executar testes com writer concorrente e query lenta**

Run: `.venv/bin/python -m pytest tests/integration/test_operational_snapshot.py tests/integration/test_operational_query_deadline.py -v`

Expected: PASS; query interrompida antes de 10,5s gera `SnapshotDeadlineExceeded` e não tenta novamente nessa execução.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/persistence/operational_reader.py tests/integration/test_operational_snapshot.py tests/integration/test_operational_query_deadline.py
git commit -m "feat: capture read-only operational snapshots"
```

### Task 4: Agendamento de quatro horas e lifecycle do batch

**Files:**
- Create: `src/statistical_analyst/scheduler.py`
- Modify: `src/statistical_analyst/persistence/analysis_store.py`
- Create: `tests/unit/test_analyst_scheduler.py`
- Create: `tests/integration/test_batch_recovery.py`

**Interfaces:**
- Consumes: `Clock`, `AnalysisStore`, callback `run_batch(WindowKey)`.
- Produces: `next_analysis_trigger(now,tz)`, `due_batch(now)`, `AnalystScheduler.run`; uma execução ativa.

- [ ] **Step 1: Escrever testes dos seis horários e +2 minutos**

```python
# tests/unit/test_analyst_scheduler.py
@pytest.mark.parametrize(
    ("now_utc", "expected_end_utc", "expected_trigger_utc"),
    [
        ("2026-08-12T05:00:00Z", "2026-08-12T03:00:00Z", "2026-08-12T07:02:00Z"),
        ("2026-08-12T07:01:59Z", "2026-08-12T07:00:00Z", "2026-08-12T07:02:00Z"),
        ("2026-08-12T07:02:00Z", "2026-08-12T07:00:00Z", "2026-08-12T11:02:00Z"),
    ],
)
def test_trigger_alignment(now_utc, expected_end_utc, expected_trigger_utc) -> None:
    result = schedule_state(parse(now_utc), ZoneInfo("America/Fortaleza"))
    assert result.latest_closed_window == parse(expected_end_utc)
    assert result.next_trigger == parse(expected_trigger_utc)
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_analyst_scheduler.py tests/integration/test_batch_recovery.py -v`

Expected: FAIL por scheduler ausente.

- [ ] **Step 3: Implementar scheduler idempotente e política de atraso**

```python
class AnalystScheduler:
    def run(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            state = schedule_state(self._clock.utc_now(), self._timezone)
            if state.trigger_due and self._single_run_lock.acquire(blocking=False):
                try:
                    self._run_batch(WindowKey(state.latest_closed_window, ANALYSIS_VERSION))
                finally:
                    self._single_run_lock.release()
            stop_event.wait(min(state.seconds_to_next_trigger, 60.0))
```

Se batch encerrado não existe, criá-lo. Se possui pacote selado, reutilizá-lo. Se está atrasado além do próximo `window_end`, terminar com fallback local sem API. Nunca processar janela ainda aberta. O banco, e não o lock em memória, impede duplicação após dois processos acidentais.

- [ ] **Step 4: Executar testes de restart, atraso e concorrência**

Run: `.venv/bin/python -m pytest tests/unit/test_analyst_scheduler.py tests/integration/test_batch_recovery.py -v`

Expected: PASS; dois schedulers concorrentes produzem um batch e um relatório lógico.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/scheduler.py src/statistical_analyst/persistence/analysis_store.py tests/unit/test_analyst_scheduler.py tests/integration/test_batch_recovery.py
git commit -m "feat: schedule idempotent four-hour analysis batches"
```

### Task 5: Partição de eventos, slots e suficiência

**Files:**
- Create: `src/statistical_analyst/analysis/windows.py`
- Create: `src/statistical_analyst/analysis/slot_selection.py`
- Create: `src/statistical_analyst/analysis/sufficiency.py`
- Create: `tests/unit/test_slot_selection.py`
- Create: `tests/unit/test_sufficiency.py`

**Interfaces:**
- Consumes: `OperationalSnapshot`.
- Produces: `window_bounds(key)`, `select_slots(events, start, end) -> SelectedSeries`, `classify_window(series) -> Sufficiency`; cada `OperationalEvent` elegível carrega sua medição associada pelo snapshot reader.

- [ ] **Step 1: Escrever testes das categorias e fronteiras 35/36/43/44**

```python
# tests/unit/test_slot_selection.py
def test_event_partition_and_latest_candidate_per_slot(make_events) -> None:
    series = select_slots(
        make_events(retained=1, already_committed_redelivery=1, invalid=1, valid_same_slot=[("00:01", 1.1), ("00:04", 1.2)]),
        start=parse("2026-08-12T00:00:00Z"), end=parse("2026-08-12T04:00:00Z"),
    )
    assert series.counts == EventCounts(received=5, retained=1, duplicate=1, invalid=1, valid_candidate=2, valid_slot=1, extra_valid_same_slot=1)
    assert series.samples[0].pressure_bar == 1.2
    assert 0 <= series.coverage_ratio <= 1

def test_first_local_dup_is_valid_candidate_once(make_events) -> None:
    series = select_slots(make_events(first_local_dup=1), start=START, end=END)
    assert (series.counts.duplicate, series.counts.valid_candidate) == (1, 1)
    assert len(series.samples) == 1

# tests/unit/test_sufficiency.py
@pytest.mark.parametrize(("slots", "expected"), [(35,"insufficient"),(36,"partial"),(43,"partial"),(44,"complete")])
def test_four_hour_coverage_boundaries(slots, expected, spread_series) -> None:
    assert classify_window(spread_series(valid_slots=slots, expected_slots=48)).value == expected
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_slot_selection.py tests/unit/test_sufficiency.py -v`

Expected: FAIL por módulos ausentes.

- [ ] **Step 3: Implementar precedência, semiabertura e regras temporais**

```python
def event_category(event: OperationalEvent) -> EventCategory:
    if event.retain:
        return EventCategory.RETAINED_EXCLUDED
    if event.delivery_disposition == "ALREADY_COMMITTED":
        return EventCategory.DUPLICATE_EXCLUDED
    if event.decode_status in {"INVALID", "QUARANTINED"}:
        return EventCategory.INVALID
    return EventCategory.VALID_CANDIDATE

def slot_index(received_at: datetime, start: datetime) -> int:
    return int((received_at - start).total_seconds() // 300)
```

Somente `start <= received_at < end`. `retain` e `delivery_disposition=ALREADY_COMMITTED` ficam fora do denominador; o bit bruto `duplicate` continua contado para auditoria, mas `FIRST_LOCAL_COPY` é candidato válido uma vez porque foi a única cópia commitada daquela publicação. Para cada slot, maior `(received_at,event_id)`. Validar invariantes da spec e falhar pacote se não fecharem. `complete`: >=90%, primeira <=10min, última >=end-10min, gap máximo <=10min. `partial`: >=75%, primeira <=30min, última >=end-10min, gap máximo <=30min. Testar também 24h em 215/216/259/260 e 7 dias em 3/4/5/6 utilizáveis e quantidade complete.

- [ ] **Step 4: Executar testes de propriedade e limites inclusivos**

Run: `.venv/bin/python -m pytest tests/unit/test_slot_selection.py tests/unit/test_sufficiency.py -v`

Expected: PASS, inclusive gap exatamente no limite e um segundo acima.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/analysis/windows.py src/statistical_analyst/analysis/slot_selection.py src/statistical_analyst/analysis/sufficiency.py tests/unit/test_slot_selection.py tests/unit/test_sufficiency.py
git commit -m "feat: select sufficient five-minute analysis series"
```

### Task 6: Estatísticas descritivas e lacunas observacionais

**Files:**
- Create: `src/statistical_analyst/analysis/descriptive.py`
- Create: `tests/unit/test_descriptive_statistics.py`
- Create: `tests/fixtures/golden/descriptive_cases.json`

**Interfaces:**
- Consumes: `SelectedSeries` da Task 5.
- Produces: `describe(series,start,end) -> DescriptiveMetrics`, `percentile_type7(values,p)` e `observation_gap_minutes(samples,start,end)`.

- [ ] **Step 1: Escrever testes n=0/1/2/3, tipo 7 e MAD**

```python
# tests/unit/test_descriptive_statistics.py
def test_empty_series_has_null_values_and_full_observation_gap() -> None:
    result = describe([], START, END)
    assert result.mean is None and result.sample_stddev is None
    assert result.observation_gap_minutes == 240.0

def test_one_value_has_defined_location_and_null_stddev() -> None:
    result = describe([sample(START + minutes(5), 1.1)], START, END)
    assert (result.mean, result.median, result.minimum, result.maximum) == (1.1, 1.1, 1.1, 1.1)
    assert result.range == 0 and result.mad_raw == 0 and result.net_change == 0
    assert result.sample_stddev is None

def test_type7_and_sample_stddev_match_golden_fixture() -> None:
    result = describe(load_case("five_values"), START, END)
    assert result.p05 == pytest.approx(1.02)
    assert result.p95 == pytest.approx(1.38)
    assert result.sample_stddev == pytest.approx(0.158113883)

def test_gap_sums_only_excess_over_ten_minutes_including_edges() -> None:
    values = [sample(START + minutes(15), 1.1), sample(START + minutes(35), 1.1), sample(END - minutes(5), 1.1)]
    assert observation_gap_minutes(values, START, END) == pytest.approx(205.0)
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_descriptive_statistics.py -v`

Expected: FAIL por módulo ausente.

- [ ] **Step 3: Implementar fórmulas explicitamente**

```python
def percentile_type7(values: Sequence[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    h = (len(ordered) - 1) * p
    lower = floor(h); upper = ceil(h)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (h - lower) * (ordered[upper] - ordered[lower])

def observation_gap_minutes(samples, start, end) -> float:
    if not samples:
        return (end - start).total_seconds() / 60
    points = [start, *(s.received_at for s in samples), end]
    return sum(max(0.0, (b - a).total_seconds() - 600.0) for a, b in pairwise(points)) / 60
```

Usar `statistics.mean`, `median`, `stdev` (n-1), MAD bruto sem fator, sem arredondamento. Com n=2, stddev definido e inclinações não pertencem a este módulo. Idade da última leitura é relativa a `end`.

- [ ] **Step 4: Executar fixtures e comparar JSON dourado**

Run: `.venv/bin/python -m pytest tests/unit/test_descriptive_statistics.py -v`

Expected: PASS para n=0,1,2,>=3, bordas e um segundo acima de 10 minutos.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/analysis/descriptive.py tests/unit/test_descriptive_statistics.py tests/fixtures/golden/descriptive_cases.json
git commit -m "feat: calculate deterministic descriptive metrics"
```

### Task 7: Tendências, deadband e ETA referenciada à janela

**Files:**
- Create: `src/statistical_analyst/analysis/trends.py`
- Create: `tests/unit/test_analysis_trends.py`
- Create: `tests/fixtures/golden/trend_cases.json`

**Interfaces:**
- Consumes: amostras selecionadas e `window_end`.
- Produces: `analyze_trend(samples,window_end) -> TrendMetrics` com `slope_4h`, `recent_slope`, classe e ETAs.

- [ ] **Step 1: Escrever testes das duas inclinações e idade 9/10/>10**

```python
# tests/unit/test_analysis_trends.py
def test_recent_slope_uses_at_most_seven_samples_in_final_thirty_minutes() -> None:
    result = analyze_trend(load_case("nine_recent_samples"), WINDOW_END)
    assert result.recent_sample_count == 7
    assert result.recent_slope_bar_per_minute == pytest.approx(-0.002)

@pytest.mark.parametrize(("age_minutes", "available"), [(9, True), (10, True), (10 + 1/60, False)])
def test_recent_slope_age_boundary(age_minutes, available) -> None:
    result = analyze_trend(series_ending(age_minutes_before_end=age_minutes), WINDOW_END)
    assert (result.recent_slope_bar_per_minute is not None) is available

def test_eta_is_measured_at_window_end_not_last_sample() -> None:
    result = analyze_trend(falling_series(last_at=WINDOW_END - minutes(9), last_bar=1.04, slope=-0.002), WINDOW_END)
    assert result.eta_low_minutes == pytest.approx(11.0)

def test_deadband_is_stable_at_exact_point_zero_one_per_fifteen() -> None:
    result = classify_slope(0.01 / 15)
    assert result == "stable"

def test_eta_is_zero_when_limit_is_already_crossed_even_with_deadband() -> None:
    assert eta_at_window_end(TimedValue(WINDOW_END, 0.99), slope=0.0, target=1.0, window_end=WINDOW_END, direction="low") == 0.0

def test_eta_is_null_for_opposite_direction_or_stale_sample() -> None:
    assert eta_at_window_end(TimedValue(WINDOW_END, 1.10), slope=0.002, target=1.0, window_end=WINDOW_END, direction="low") is None
    assert eta_at_window_end(TimedValue(WINDOW_END - minutes(11), 1.10), slope=-0.002, target=1.0, window_end=WINDOW_END, direction="low") is None
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_analysis_trends.py -v`

Expected: FAIL por módulo ausente.

- [ ] **Step 3: Implementar Theil–Sen e ETA exatos**

```python
def eta_at_window_end(last: TimedValue, slope: float, target: float,
                      window_end: datetime, direction: Literal["low", "high"]) -> float | None:
    crossed = last.value <= target if direction == "low" else last.value >= target
    if crossed:
        return 0.0
    if window_end - last.received_at > timedelta(minutes=10):
        return None
    if abs(slope * 15) <= 0.01:
        return None
    if (direction == "low" and slope >= 0) or (direction == "high" and slope <= 0):
        return None
    minutes_from_last = (target - last.value) / slope
    if minutes_from_last < 0:
        return None
    crossing_at = last.received_at + timedelta(minutes=minutes_from_last)
    eta = max(0.0, (crossing_at - window_end).total_seconds() / 60)
    return eta if eta <= 1440 else None
```

`slope_4h`: >=3 amostras, span >=30m, todos os pares com dt>0. `recent_slope`: últimas até 7 nos 30m finais, >=3, span >=10m, última idade <=10m. Classificar rising se `slope*15 > .01`, falling se `<-.01`, stable inclusivo, uncertain se null. ETA zero se último valor já cruzou ou crossing_at <=end; null se direção oposta, stale ou >1440.

- [ ] **Step 4: Executar matriz dourada de tendências**

Run: `.venv/bin/python -m pytest tests/unit/test_analysis_trends.py -v`

Expected: PASS, incluindo timestamps iguais, ETA zero/null/válida e horizonte.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/analysis/trends.py tests/unit/test_analysis_trends.py tests/fixtures/golden/trend_cases.json
git commit -m "feat: project deterministic trend and ETA metrics"
```

### Task 8: Referências de 24 horas e 7 dias

**Files:**
- Create: `src/statistical_analyst/analysis/historical.py`
- Create: `tests/unit/test_historical_comparison.py`
- Create: `tests/fixtures/golden/historical_cases.json`

**Interfaces:**
- Consumes: métricas diárias e suficiência das Tasks 5–7.
- Produces: `build_historical_reference(...) -> HistoricalMetrics`, robust z e classe de variabilidade.

- [ ] **Step 1: Escrever testes de não sobreposição e pesos iguais**

```python
# tests/unit/test_historical_comparison.py
def test_previous_twenty_four_hours_excludes_current_four_hours() -> None:
    bounds = historical_bounds(WINDOW_END, ZoneInfo("America/Fortaleza"))
    assert bounds.previous_24h == Interval(WINDOW_END - hours(28), WINDOW_END - hours(4))
    assert bounds.current_4h.start == bounds.previous_24h.end

def test_seven_day_reference_weights_days_not_samples() -> None:
    days = [daily_metric(median=1.0, slots=48), daily_metric(median=1.2, slots=36)]
    result = aggregate_daily(days)
    assert result.median_of_daily_medians == 1.1

@pytest.mark.parametrize(("usable", "complete", "expected"), [(3,3,"insufficient"),(4,3,"partial"),(6,3,"partial"),(6,4,"complete")])
def test_seven_day_sufficiency(usable, complete, expected) -> None:
    assert classify_seven_days(usable, complete).value == expected

def test_robust_z_boundary_and_zero_reference_variability() -> None:
    assert robust_location_flag(3.5) is True
    assert robust_location_flag(-3.5) is True
    reference = historical_reference(usable_days=4, complete_days=3, daily_stddevs=[0.0] * 4)
    assert classify_variability(current_stddev=4/4096, reference=reference) == "expected"
    assert classify_variability(current_stddev=4/4096 + 1e-9, reference=reference) == "higher"

def test_insufficient_reference_never_emits_robust_z_or_variability() -> None:
    reference = historical_reference(usable_days=3, complete_days=3, daily_medians=[1.0, 1.1, 1.2])
    assert robust_z(1.3, reference) is None
    assert classify_variability(0.02, reference) == "uncertain"

def test_zero_historical_mad_makes_robust_z_null_even_when_reference_usable() -> None:
    reference = historical_reference(usable_days=4, complete_days=3, daily_medians=[1.1] * 4)
    assert robust_z(1.2, reference) is None
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_historical_comparison.py -v`

Expected: FAIL por módulo ausente.

- [ ] **Step 3: Implementar agregação diária e fórmulas robustas**

```python
def robust_z(current_median: float, reference: HistoricalReference) -> float | None:
    if reference.sufficiency == "insufficient":
        return None
    center = median(reference.daily_medians)
    mad = median(abs(value - center) for value in reference.daily_medians)
    return None if mad == 0 else 0.67448975 * (current_median - center) / mad

def classify_variability(current_stddev: float | None, reference: HistoricalReference) -> str:
    historical_median_stddev = reference.median_daily_stddev
    if reference.sufficiency == "insufficient" or current_stddev is None or historical_median_stddev is None:
        return "uncertain"
    if historical_median_stddev == 0:
        return "expected" if current_stddev <= 4 / 4096 else "higher"
    ratio = current_stddev / historical_median_stddev
    return "lower" if ratio < 0.67 else "higher" if ratio > 1.50 else "expected"
```

As sete janelas devem ser calculadas por mesmo intervalo local usando `ZoneInfo`, depois convertidas a UTC. Toda frequência carrega numerador e denominador (`usable_day_count` ou `valid_slot_count`). Nenhuma inferência de correlação entre reservatórios.

- [ ] **Step 4: Executar testes históricos e mudança de data**

Run: `.venv/bin/python -m pytest tests/unit/test_historical_comparison.py -v`

Expected: PASS para 3/4/5/6 dias, 3/4 completos, robust z abaixo/exato/acima 3.5 e razões 0.67/1.50 inclusivas.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/analysis/historical.py tests/unit/test_historical_comparison.py tests/fixtures/golden/historical_cases.json
git commit -m "feat: compare independent historical reservoir windows"
```

### Task 9: Duração de estados e classificação local

**Files:**
- Create: `src/statistical_analyst/analysis/state_durations.py`
- Create: `src/statistical_analyst/analysis/classification.py`
- Create: `tests/unit/test_state_durations.py`
- Create: `tests/unit/test_local_classification.py`

**Interfaces:**
- Consumes: state transitions, suficiência, tendências, histórico e alarmes locais.
- Produces: `state_durations(transitions,machine,start,end)`, `state_durations_by_machine(...)`, `classify_analysis(package) -> LocalClassification`.

- [ ] **Step 1: Escrever testes de retenção à esquerda, soma e prioridade**

```python
# tests/unit/test_state_durations.py
def test_durations_use_effective_time_and_left_carry() -> None:
    transitions = [
        transition("quality", "GOOD", effective=START - minutes(5), recorded=START + minutes(1)),
        transition("quality", "MISSING", effective=START + minutes(15), recorded=START + minutes(20)),
        transition("quality", "GOOD", effective=START + minutes(30), recorded=START + minutes(31)),
    ]
    result = state_durations(transitions, "quality", START, END)
    assert result["GOOD"] == pytest.approx(225.0)
    assert result["MISSING"] == pytest.approx(15.0)
    assert sum(result.values()) == pytest.approx(240.0)

def test_quality_and_process_each_partition_the_full_window() -> None:
    transitions = [
        transition("quality", "GOOD", effective=START - minutes(5)),
        transition("process", "NORMAL", effective=START - minutes(7)),
        transition("quality", "MISSING", effective=START + minutes(15)),
        transition("process", "LOW_WARNING", effective=START + minutes(120)),
    ]
    result = state_durations_by_machine(transitions, START, END)
    assert sum(result["quality"].values()) == pytest.approx(240.0)
    assert sum(result["process"].values()) == pytest.approx(240.0)
    assert result["quality"]["MISSING"] == pytest.approx(225.0)
    assert result["process"]["LOW_WARNING"] == pytest.approx(120.0)

def test_duration_sum_outside_one_second_invalidates_package() -> None:
    with pytest.raises(DurationInvariantError):
        validate_duration_sum({"GOOD": 239.98})

# tests/unit/test_local_classification.py
def test_critical_alarm_has_immutable_immediate_priority(complete_package) -> None:
    result = classify_analysis(complete_package.with_critical_alarm())
    assert result.local_report_priority == "immediate_human_review"

def test_complete_requires_all_three_references_complete(complete_package) -> None:
    assert classify_analysis(complete_package).analysis_status == "complete"
    assert classify_analysis(complete_package.with_24h("partial")).analysis_status == "limited"
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_state_durations.py tests/unit/test_local_classification.py -v`

Expected: FAIL por módulos ausentes.

- [ ] **Step 3: Implementar intervalos e classificações fechadas**

```python
def validate_duration_sum(durations: Mapping[str, float]) -> None:
    if abs(sum(durations.values()) - 240.0) > (1.0 / 60.0):
        raise DurationInvariantError(sum(durations.values()))

def report_priority(active_alarms, quality, eta, robust_shift, variability) -> str:
    if any(a.severity == "CRITICAL" for a in active_alarms):
        return "immediate_human_review"
    if active_alarms or quality != "GOOD" or (eta is not None and eta <= 15) or robust_shift or variability == "higher":
        return "priority"
    return "routine"
```

Usar estado imediatamente anterior ao start como carry de cada máquina, recortar transições e incluir todos os estados, inclusive UNKNOWN/INITIALIZING. Validar a soma de 240 minutos com tolerância de um segundo separadamente para `quality` e `process`; ausência de carry de qualquer máquina invalida o pacote. Status: complete somente três referências complete; limited para janela atual complete/partial; fallback_required para atual insufficient/invariantes. Confidence conforme spec, sempre local. Gerar evidence IDs estáveis como `current.median`, `trend.eta_low`, `quality.duration.stuck`.

- [ ] **Step 4: Executar testes de todas as combinações locais**

Run: `.venv/bin/python -m pytest tests/unit/test_state_durations.py tests/unit/test_local_classification.py -v`

Expected: PASS; nenhuma API/model aparece nesses módulos.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/analysis/state_durations.py src/statistical_analyst/analysis/classification.py tests/unit/test_state_durations.py tests/unit/test_local_classification.py
git commit -m "feat: classify analysis status and local priority"
```

### Task 10: Package builder, allowlist e selagem antes da chamada ao modelo

**Files:**
- Create: `src/statistical_analyst/analysis/package_builder.py`
- Create: `src/statistical_analyst/security.py`
- Modify: `src/statistical_analyst/persistence/analysis_store.py`
- Create: `tests/unit/test_package_builder.py`
- Create: `tests/unit/test_security_scan.py`
- Create: `tests/integration/test_sealed_input_restart.py`

**Interfaces:**
- Consumes: todas as métricas e `SnapshotManifest`.
- Produces: Pydantic `AnalysisPackage(extra='forbid')`, `canonical_bytes()`, `scan_public_payload()`, `seal_input()`.

- [ ] **Step 1: Escrever testes de bytes canônicos, limite e segredos**

```python
# tests/unit/test_package_builder.py
def test_package_bytes_and_hash_are_deterministic(package_factory) -> None:
    first = package_factory(order="forward").canonical_bytes()
    second = package_factory(order="reverse").canonical_bytes()
    assert first == second
    assert sha256(first).hexdigest() == sha256(second).hexdigest()
    assert len(first) <= 16_000

def test_package_rejects_unknown_field(package_dict) -> None:
    with pytest.raises(ValidationError):
        AnalysisPackage.model_validate({**package_dict, "operator_email": "operator@example.com"})

# tests/integration/test_sealed_input_restart.py
def test_next_snapshot_counts_real_late_arrival_without_mutating_closed_input(
    store, reader, core_writer, package_builder, first_window_key, next_window_key
) -> None:
    batch = store.get_or_create_batch(first_window_key)
    first_snapshot = reader.capture(first_window_key, deadline_monotonic=999999)
    first_package = package_builder.build("0051", first_snapshot)
    closed = store.seal_input(batch.batch_id, "0051", first_package, first_snapshot.manifest)
    core_writer.insert_measurement(
        event_id="late-e2",
        received_at=first_window_key.window_end_utc - minutes(1),
        persisted_at=first_snapshot.manifest.snapshot_at_utc + seconds(1),
    )
    next_snapshot = reader.capture(next_window_key, deadline_monotonic=999999)
    next_package = package_builder.build("0051", next_snapshot, prior_sealed_inputs=[closed])
    reloaded = store.load_sealed_input(batch.batch_id, "0051")
    assert reloaded.canonical_json == closed.canonical_json
    assert next_package.current_4h.late_arrival_after_snapshot_count == 1

# tests/unit/test_security_scan.py
@pytest.mark.parametrize("value", ["sk-live-secret", "Bearer abc.def", "operator@example.com", "password=abc"])
def test_security_scan_blocks_sensitive_text(value) -> None:
    with pytest.raises(SensitivePayloadError):
        scan_public_payload({"metric": value})
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_package_builder.py tests/unit/test_security_scan.py tests/integration/test_sealed_input_restart.py -v`

Expected: FAIL por package builder ausente.

- [ ] **Step 3: Implementar Pydantic fechado e serialização estável**

```python
class AnalysisPackage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    contract_version: Literal["1.0"]
    prompt_version: Literal["statistical-analyst-v1"]
    analysis_run_id: UUID
    reservoir_id: ReservoirId
    topic: str
    window: WindowDescriptor
    limits: LocalLimits
    local_classification: LocalClassificationModel
    current_4h: WindowMetrics
    previous_24h: WindowMetrics
    same_time_7d: HistoricalMetricsModel
    active_alarm_refs: list[str]
    evidence_catalog: dict[str, EvidenceValue]

    def canonical_bytes(self) -> bytes:
        return json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
```

`WindowMetrics` inclui todas as contagens da partição e `late_arrival_after_snapshot_count`. Para calculá-lo na execução seguinte, comparar eventos agora visíveis e com `received_at` dentro de janelas previamente seladas contra os `event_ids` de seus manifestos; contar cada `event_id` novo uma vez, sem reabrir ou reenviar o batch antigo. `LocalLimits`, tópicos e `rule_version` vêm exclusivamente do manifesto `operational-v1` já validado no snapshot; não existem constantes analíticas paralelas. `build_package` não recebe recipients ou credenciais. Scan antes da persistência e envio. `seal_input(batch_id,reservoir_id,package,manifest)` usa transação INSERT-once; se já existe, compara hash e retorna bytes originais ou abre erro de integridade. Persistir snapshot_at, event IDs ordenados, contagens, contrato/hash e bytes antes do HTTP. Late arrivals não mutam entrada selada.

- [ ] **Step 4: Executar testes de restart e corrida de late arrival**

Run: `.venv/bin/python -m pytest tests/unit/test_package_builder.py tests/unit/test_security_scan.py tests/integration/test_sealed_input_restart.py -v`

Expected: PASS; restart devolve bytes idênticos e evento tardio aparece somente em manifesto futuro.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/analysis/package_builder.py src/statistical_analyst/security.py src/statistical_analyst/persistence/analysis_store.py tests/unit/test_package_builder.py tests/unit/test_security_scan.py tests/integration/test_sealed_input_restart.py
git commit -m "feat: seal allowlisted analysis packages"
```

### Task 11: Contrato Pydantic de saída, prompt e catálogo

**Files:**
- Modify: `pyproject.toml`
- Create: `src/statistical_analyst/domain/claude_contract.py`
- Create: `src/statistical_analyst/ai/prompt.py`
- Create: `resources/prompts/statistical_analyst_v1.txt`
- Create: `resources/catalogs/action_codes_v1.json`
- Create: `tests/unit/test_claude_schema.py`
- Create: `tests/unit/test_prompt_catalog.py`
- Create: `tests/fixtures/golden/claude_schema_v1.json`
- Modify: `tests/integration/test_analyst_wheel_artifact.py`

**Interfaces:**
- Consumes: evidence IDs e versão da Task 10.
- Produces: `AnalysisResponse`, `Finding`, `RecommendedCheck`, `canonical_claude_schema_v1()` gerado do Pydantic, `load_prompt_v1(resource_directory)` e catálogo fechado.

- [ ] **Step 1: Escrever testes do schema e catálogo exatos**

```python
# tests/unit/test_claude_schema.py
def test_generated_schema_matches_reviewed_fixture() -> None:
    schema = canonical_claude_schema_v1()
    assert schema == load_json("tests/fixtures/golden/claude_schema_v1.json")
    assert_all_objects_closed_and_required(schema)
    assert "uniqueItems" not in json.dumps(schema)

def test_response_limits_and_duplicate_evidence_are_local_validation() -> None:
    with pytest.raises(ValidationError):
        AnalysisResponse.model_validate(valid_response(findings=[finding()] * 6))
    with pytest.raises(ValidationError, match="duplicate"):
        AnalysisResponse.model_validate(valid_response(findings=[finding(evidence_refs=["a","a"])]))

@pytest.mark.parametrize("bad", [
    "12345678-1234-1234-8234-123456789abc",  # UUID v1
    "12345678-1234-4234-8234-123456789ABC",  # uppercase
])
def test_analysis_run_id_requires_lowercase_uuid_v4_pattern(bad: str) -> None:
    with pytest.raises(ValidationError):
        AnalysisResponse.model_validate(valid_response(analysis_run_id=bad))

# tests/unit/test_prompt_catalog.py
def test_catalog_contains_only_approved_action_codes(resource_directory) -> None:
    assert set(load_action_catalog(resource_directory)) == {
        "NO_ACTION_MONITOR", "VERIFY_SENSOR", "COMPARE_FIELD_GAUGE",
        "INSPECT_COMMUNICATION", "REVIEW_PROCESS_TREND", "ESCALATE_TO_OPERATOR",
    }

def test_prompt_and_catalog_load_from_absolute_resource_directory(resource_directory, tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    assert load_prompt_v1(resource_directory).version == "statistical-analyst-v1"
    assert load_action_catalog(resource_directory)

# tests/integration/test_analyst_wheel_artifact.py
def test_isolated_wheel_contains_all_versioned_runtime_resources(isolated_wheel_install) -> None:
    probe = isolated_wheel_install.run_resource_probe(cwd=isolated_wheel_install.empty_directory)
    assert probe.data_file("share/agente-mqtt/contracts/operational_v1.json").exists
    assert probe.data_file("share/agente-mqtt/statistical-analyst/prompts/statistical_analyst_v1.txt").exists
    assert probe.data_file("share/agente-mqtt/statistical-analyst/catalogs/action_codes_v1.json").exists
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_claude_schema.py tests/unit/test_prompt_catalog.py tests/integration/test_analyst_wheel_artifact.py -v`

Expected: FAIL por contrato ausente.

- [ ] **Step 3: Implementar modelos fechados e recursos versionados**

```python
EvidenceRef = Annotated[str, StringConstraints(pattern=r"^[a-z0-9_.-]{1,64}$")]
LowerUuid4 = Annotated[
    str,
    StringConstraints(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"),
]

class Finding(BaseModel):
    model_config = ConfigDict(extra="forbid")
    category: Literal["trend", "variability", "data_quality", "threshold_proximity", "historical_change"]
    title: Annotated[str, StringConstraints(min_length=1, max_length=120)]
    explanation: Annotated[str, StringConstraints(min_length=1, max_length=400)]
    evidence_refs: Annotated[list[EvidenceRef], Field(min_length=1, max_length=5)]

    @field_validator("evidence_refs")
    @classmethod
    def unique_refs(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate evidence reference")
        return value

class AnalysisResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["1.0"]
    analysis_run_id: LowerUuid4
    reservoir_id: ReservoirId
    executive_summary: Annotated[str, StringConstraints(min_length=1, max_length=600)]
    trend_interpretation: Annotated[str, StringConstraints(min_length=1, max_length=400)]
    variability_interpretation: Annotated[str, StringConstraints(min_length=1, max_length=400)]
    findings: Annotated[list[Finding], Field(max_length=5)]
    recommended_checks: Annotated[list[RecommendedCheck], Field(max_length=3)]
    limitations: Annotated[list[Annotated[str, StringConstraints(min_length=1, max_length=240)]], Field(max_length=5)]
```

Acrescentar as duas chaves abaixo à tabela `[tool.setuptools.data-files]` já criada pelo Core, sem redeclarar o cabeçalho TOML:

```toml
"share/agente-mqtt/statistical-analyst/prompts" = ["resources/prompts/statistical_analyst_v1.txt"]
"share/agente-mqtt/statistical-analyst/catalogs" = ["resources/catalogs/action_codes_v1.json"]
```

`AnalysisResponse` é a fonte única do schema: `canonical_claude_schema_v1()` chama `model_json_schema()`, normaliza deterministicamente para o subconjunto de JSON Schema aceito por `claude -p --json-schema` na versão fixada do CLI (objetos fechados, todos os campos `required`, sem `uniqueItems`) e calcula o hash versionado. `tests/fixtures/golden/claude_schema_v1.json` é somente uma fixture de regressão gerada/comparada no teste, não um arquivo de runtime nem uma segunda fonte. O prompt deve ordenar: usar apenas métricas; não recalcular; não relacionar reservatórios; aceitar status/tendência/ETA/prioridade locais; sem comandos; output apenas no schema; narrativa sem algarismos. Os loaders recebem o `resource_directory` absoluto, nunca consultam CWD, calculam SHA-256 e falham se versão/hash não corresponder ao catálogo de recursos esperado. Reinstalar o editable package e testar a presença de contrato, prompt e catálogo no data path do venv.

- [ ] **Step 4: Executar testes e validar schema contra o subconjunto aceito pelo `--json-schema`**

Run: `.venv/bin/python -m pytest tests/unit/test_claude_schema.py tests/unit/test_prompt_catalog.py tests/integration/test_analyst_wheel_artifact.py -v`

Expected: PASS; todos os campos required, `additionalProperties=false` em cada objeto, enums e maxItems corretos.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml src/statistical_analyst/domain/claude_contract.py src/statistical_analyst/ai/prompt.py resources tests/unit/test_claude_schema.py tests/unit/test_prompt_catalog.py tests/integration/test_analyst_wheel_artifact.py tests/fixtures/golden/claude_schema_v1.json
git commit -m "feat: define strict consultative Claude contract"
```

### Task 12: Validador do envelope do Claude Code e fallback semântico

**Files:**
- Create: `src/statistical_analyst/ai/cli_envelope.py`
- Create: `src/statistical_analyst/ai/response_validator.py`
- Create: `tests/fixtures/claude_cli/success_v1.json`
- Create: `tests/fixtures/claude_cli/error_variants_v1.json`
- Create: `tests/unit/test_cli_envelope.py`
- Create: `tests/unit/test_response_validator.py`

**Interfaces:**
- Consumes: `AnalysisResponse`, `SealedInput` e os bytes de `stdout` do `claude -p --output-format json`.
- Produces: `parse_cli_envelope(stdout: bytes, exit_code: int) -> CliEnvelope` e `validate_response(envelope, sealed) -> ValidatedInterpretation | ValidationFailure`; nenhuma repetição.

As fixtures em `tests/fixtures/claude_cli/` são **capturadas da versão fixada do CLI** (Task 19, teste live) e revisadas manualmente: chaves, tipos e exemplos de `subtype` de erro. Trocar a versão do CLI exige recapturar e revisar as fixtures.

- [ ] **Step 1: Escrever a matriz de estados do envelope**

```python
# tests/unit/test_cli_envelope.py
def test_parse_success_fixture_keeps_raw_fields(success_stdout) -> None:
    env = parse_cli_envelope(success_stdout, exit_code=0)
    assert env.type == "result" and env.subtype == "success" and env.is_error is False
    assert env.structured_output is not None
    assert env.session_id and env.total_cost_usd is not None
    assert env.usage.input_tokens >= 0 and env.usage.output_tokens >= 0
    assert set(env.model_usage) == {"claude-sonnet-5-5"}

@pytest.mark.parametrize("stdout", [b"", b"not json", b"[]", b'{"type":"assistant"}'])
def test_unparseable_stdout_is_envelope_error(stdout) -> None:
    env = parse_cli_envelope(stdout, exit_code=0)
    assert env.parse_error is not None

def test_stdout_over_64_kib_is_rejected_without_parsing() -> None:
    assert parse_cli_envelope(b"{" + b" " * 70_000 + b"}", exit_code=0).parse_error == "STDOUT_TOO_LARGE"

# tests/unit/test_response_validator.py
@pytest.mark.parametrize("envelope", [
    envelope(exit_code=1),
    envelope(is_error=True, subtype="error_during_execution"),
    envelope(subtype="error_max_budget_usd"),
    envelope(subtype="error_max_turns"),
    envelope(structured_output=None),
    envelope(permission_denials=[{"tool_name": "Read"}]),
    envelope(tool_uses=1),
    envelope(model_usage={"claude-sonnet-5-5": {}, "claude-haiku-5-5": {}}),
    envelope(model_usage={"claude-opus-5-5": {}}),
    envelope(usage=None),
])
def test_noncanonical_envelope_is_fallback(envelope, sealed_input) -> None:
    result = validate_response(envelope, sealed_input)
    assert isinstance(result, ValidationFailure)
    assert result.retry_allowed is False

def test_free_text_result_is_ignored_when_structured_output_is_valid(sealed_input) -> None:
    result = validate_response(valid_envelope(result_text="qualquer texto 123"), sealed_input)
    assert isinstance(result, ValidatedInterpretation)

@pytest.mark.parametrize("text", ["nível 1", "abrir válvula", "parar bomba", "setpoint", "override"])
def test_digits_and_control_language_are_rejected(text, sealed_input) -> None:
    result = validate_response(valid_envelope(executive_summary=text), sealed_input)
    assert isinstance(result, ValidationFailure)
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_cli_envelope.py tests/unit/test_response_validator.py -v`

Expected: FAIL por módulos ausentes.

- [ ] **Step 3: Implementar parser tolerante e validação em ordem fixa**

```python
# src/statistical_analyst/ai/response_validator.py
def validate_response(envelope: CliEnvelope, sealed: SealedInput) -> ValidationResult:
    if envelope.parse_error is not None or envelope.exit_code != 0:
        return failure("CLI_ENVELOPE_INVALID")
    if envelope.type != "result" or envelope.subtype != "success" or envelope.is_error:
        return failure("CLI_NOT_SUCCESS")
    if envelope.tool_use_count or envelope.permission_denials:
        return failure("TOOL_USE_ATTEMPTED")
    if set(envelope.model_usage) != {FIXED_MODEL}:
        return failure("MODEL_MISMATCH")
    if envelope.usage is None:
        return failure("USAGE_MISSING")
    if envelope.structured_output is None:
        return failure("STRUCTURED_OUTPUT_MISSING")
    raw = canonical_json_bytes(envelope.structured_output)
    if len(raw) > 6000:
        return failure("VISIBLE_OUTPUT_TOO_LARGE")
    try:
        parsed = AnalysisResponse.model_validate_json(raw)
    except ValidationError:
        return failure("SCHEMA_INVALID")
    if UUID(parsed.analysis_run_id) != sealed.run_id or parsed.reservoir_id != sealed.reservoir_id:
        return failure("IDENTITY_MISMATCH")
    return validate_narrative_and_evidence(parsed, sealed)
```

`parse_cli_envelope` faz o seguinte:

- limita `stdout` a 64 KiB e decodifica UTF-8 estrito;
- aceita chaves desconhecidas, que são preservadas para auditoria, mas exige `type`, `subtype`, `is_error`, `session_id`, `usage`, `total_cost_usd` e `modelUsage` com os tipos das fixtures revisadas;
- conta `tool_use_count` a partir de `num_turns`/`permission_denials` e de qualquer indicação de ferramenta presente na fixture da versão fixada.

O texto livre `result` nunca é usado: só `structured_output`, e mesmo ele passa de novo pelo Pydantic local, sem confiar no `--json-schema` do CLI.

Validar todas as refs contra o catálogo de entrada, com unicidade. Rejeitar qualquer algarismo Unicode em texto livre. Aplicar a denylist de comandos de controle com normalização casefold e conferir os action codes no catálogo. O envelope bruto fica em `analytics.sqlite` para auditoria, mas nunca é enviado se a validação falhar. `ValidationFailure.retry_allowed` é sempre false nesta Task.

- [ ] **Step 4: Executar matriz inteira**

Run: `.venv/bin/python -m pytest tests/unit/test_cli_envelope.py tests/unit/test_response_validator.py -v`

Expected: PASS para o envelope de sucesso válido; todo estado de erro, ferramenta, modelo divergente ou ausência de saída estruturada é rejeitado.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/ai/cli_envelope.py src/statistical_analyst/ai/response_validator.py tests/fixtures/claude_cli tests/unit/test_cli_envelope.py tests/unit/test_response_validator.py
git commit -m "feat: validate Claude Code envelopes before operator use"
```

### Task 13: Orçamento móvel, circuit breaker e retry policy

**Files:**
- Create: `src/statistical_analyst/ai/retry_policy.py`
- Create: `src/statistical_analyst/ai/circuit_breaker.py`
- Create: `resources/catalogs/claude_error_classes_v1.json`
- Modify: `src/statistical_analyst/persistence/usage_store.py`
- Create: `tests/unit/test_retry_policy.py`
- Create: `tests/unit/test_circuit_breaker.py`
- Create: `tests/integration/test_usage_concurrency.py`

**Interfaces:**
- Consumes: Clock, `analytics.sqlite`, `CliEnvelope` e o resultado bruto do subprocesso (`exit_code`, timeout, `stderr` já truncado).
- Produces: `UsageBudget.try_reserve/reconcile`, `classify_error`, `next_retry` e `CircuitBreaker.allow/record`.

- [ ] **Step 1: Escrever testes dos limites, do custo e da classificação**

```python
# tests/integration/test_usage_concurrency.py
def test_twenty_fourth_reservation_succeeds_and_twenty_fifth_fails(two_usage_stores, now) -> None:
    reservations = concurrent_reserve(two_usage_stores, count=25, now=now)
    assert sum(item is not None for item in reservations) == 24

def test_timeout_keeps_full_reservation_for_twenty_four_hours(usage_store, now) -> None:
    reservation = usage_store.try_reserve(uuid4(), now)
    usage_store.reconcile(reservation.reservation_id, usage=None, now=now + minutes(1))
    assert usage_store.totals(now + hours(23)).input_tokens == 5000
    assert usage_store.totals(now + hours(23)).cost_usd == usage_store.max_budget_usd_per_call
    assert usage_store.totals(now + hours(24) + seconds(1)).input_tokens == 0

def test_reconciled_usage_replaces_reserved_amount_without_losing_attempt(usage_store, now) -> None:
    reservation = usage_store.try_reserve(uuid4(), now)
    usage_store.reconcile(reservation.reservation_id,
                          usage=TokenUsage(input_tokens=900, cache_creation_input_tokens=200,
                                           cache_read_input_tokens=100, output_tokens=800, cost_usd=Decimal("0.031")),
                          now=now)
    totals = usage_store.totals(now)
    assert (totals.attempts, totals.input_tokens, totals.output_tokens) == (1, 1200, 800)
    assert totals.cost_usd == Decimal("0.031")

def test_daily_usd_budget_blocks_reservation(usage_store_with_daily_budget, now) -> None:
    usage_store_with_daily_budget.seed_cost(Decimal("0.99"), at=now - hours(1))
    assert usage_store_with_daily_budget.try_reserve(uuid4(), now) is None  # 0.99 + reserva por chamada > 1.00

# tests/unit/test_retry_policy.py
@pytest.mark.parametrize("raw", [
    subprocess_timeout(), network_failure_envelope(), overloaded_envelope(), server_error_envelope(500),
    rate_limited_envelope(retry_after_seconds=12),
])
def test_transient_failures_are_retryable(raw) -> None:
    assert classify_error(raw).retryable is True

@pytest.mark.parametrize("raw", [
    auth_failure_envelope(), not_logged_in_envelope(), credit_exhausted_envelope(),
    subscription_usage_limit_envelope(), max_budget_envelope(), unknown_error_envelope(),
    cli_version_mismatch(), cli_binary_missing(),
])
def test_auth_quota_budget_configuration_and_unknown_never_retry(raw) -> None:
    assert classify_error(raw).retryable is False

def test_classification_uses_versioned_catalog_not_free_text_guessing(resource_directory) -> None:
    catalog = load_error_catalog(resource_directory)
    assert catalog.version == "claude-error-classes-v1"
    assert catalog.cli_version == load_analyst_config_fixture().claude_cli_version

def test_retry_after_is_minimum_with_jitter_and_respects_deadline(now) -> None:
    assert next_retry(rate_limited_envelope(retry_after_seconds=12), now, jitter=2, deadline=now + seconds(30)) == now + seconds(14)
    assert next_retry(rate_limited_envelope(retry_after_seconds=60), now, jitter=0, deadline=now + seconds(30)) is None

def test_physical_retry_reserves_a_distinct_attempt_and_success_resets_transient_count(retry_harness) -> None:
    retry_harness.fail_once(subprocess_timeout()).then_succeed()
    result = retry_harness.run()
    assert result.status == "completed"
    assert retry_harness.distinct_reservation_count == 2
    assert retry_harness.breaker.consecutive_transient_failures == 0
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_retry_policy.py tests/unit/test_circuit_breaker.py tests/integration/test_usage_concurrency.py -v`

Expected: FAIL por policy/store incompletos.

- [ ] **Step 3: Implementar reserva `BEGIN IMMEDIATE` e policies puras**

```python
def try_reserve(self, attempt_id: UUID, now: datetime, input_tokens: int = 5000, output_tokens: int = 4000):
    cutoff = now - timedelta(hours=24)
    with self._transaction("IMMEDIATE") as conn:
        attempts, inputs, outputs, cost = conn.execute(
            "SELECT count(*),coalesce(sum(accounted_input_tokens),0),coalesce(sum(accounted_output_tokens),0),"
            "coalesce(sum(accounted_cost_micro_usd),0) FROM usage_reservations WHERE reserved_at_utc>?",
            (iso(cutoff),),
        ).fetchone()
        reserve_cost = to_micro_usd(self._max_budget_usd_per_call)
        if (attempts >= 24 or inputs + input_tokens > 120000 or outputs + output_tokens > 96000
                or cost + reserve_cost > to_micro_usd(self._daily_budget_usd)):
            return None
        return insert_reservation(conn, attempt_id, now, input_tokens, output_tokens, reserve_cost)
```

Adicionar à migração analítica as colunas `reserved_cost_micro_usd` e `accounted_cost_micro_usd` (INTEGER ≥ 0). Custos são guardados em micro-USD inteiros, nunca em float.

Reconciliação com `usage` confiável:

- `accounted_input_tokens = input_tokens + cache_creation_input_tokens + cache_read_input_tokens`;
- os tokens de saída e o `total_cost_usd` substituem os valores reservados;
- sem `usage`, a reserva fica integral (5.000/4.000 e `max_budget_usd_per_call`);
- se o uso real exceder qualquer reserva, ou se `subtype=error_max_budget_usd`, persiste o uso real, abre o breaker de configuração e bloqueia novas chamadas até nova validação e reinício controlado.

`classify_error` usa somente o catálogo versionado `claude_error_classes_v1.json`. Ele mapeia combinações de `exit_code`, `subtype`, timeout e padrões de mensagem **capturados da versão fixada do CLI** (fixtures da Task 12) para as classes `transient`, `auth`, `quota`, `budget`, `configuration` e `unknown`. Erro desconhecido é não transitório por padrão seguro.

Regras de retry e breaker:

- Retry: no máximo 1, só para `transient`. Se a mensagem trouxer retry-after, ele é o mínimo de espera; soma-se jitter injetável, e a espera nunca passa do deadline. Sem retry-after: exponencial + jitter ≤ 30 s.
- Breaker de 24 h para `auth` e `quota`. Isso inclui fim de cota da assinatura, que vira fallback até a próxima janela de cota sem tentativa extra.
- Breaker transitório depois de três falhas `transient` consecutivas, até a próxima janela; ele zera após um sucesso.
- `configuration`, que inclui versão divergente e binário ausente, só fecha em reinício controlado.

As retentativas internas do próprio CLI contra a API contam dentro da mesma tentativa física e do mesmo timeout de 60 s. O teste de boundary (Task 14) confirma que o tempo total do subprocesso nunca passa desse limite. Existe uma única camada de retry na aplicação, e cada tentativa física recebe `attempt_id`/reserva distintos antes de iniciar o subprocesso.

- [ ] **Step 4: Executar testes de corrida, janela móvel, custo e breaker persistente**

Run: `.venv/bin/python -m pytest tests/unit/test_retry_policy.py tests/unit/test_circuit_breaker.py tests/integration/test_usage_concurrency.py -v`

Expected: PASS. O retry físico só ocorre após uma nova reserva bem-sucedida, e o orçamento em USD bloqueia antes de o subprocesso começar.

- [ ] **Step 5: Commit**

```bash
git add resources/catalogs/claude_error_classes_v1.json src/statistical_analyst/ai/retry_policy.py src/statistical_analyst/ai/circuit_breaker.py src/statistical_analyst/persistence/usage_store.py tests/unit/test_retry_policy.py tests/unit/test_circuit_breaker.py tests/integration/test_usage_concurrency.py
git commit -m "feat: enforce Claude retry, token and cost budgets"
```

### Task 14: Adapter Claude Code headless (`claude -p`) sem ferramentas

**Files:**
- Create: `src/statistical_analyst/ai/request_builder.py`
- Create: `src/statistical_analyst/ai/claude_client.py`
- Create: `tests/unit/test_claude_request.py`
- Create: `tests/integration/test_claude_subprocess_boundary.py`
- Create: `tests/fixtures/fake_claude_cli.py`

**Interfaces:**
- Consumes: `SealedInput`, resource directory absoluto, `SecretProvider`, `AnalysisResponse`, UsageBudget, retry policy e `AnalystConfig` (`claude_cli_path`, `claude_cli_version`, `claude_workdir`, `claude_home`, `claude_credential_kind`).
- Produces: `RequestBuilder(resource_directory).prepare(sealed) -> PreparedClaudeRequest`, com guard de bytes, e `ClaudeCodeTransport.analyze(prepared, client_request_id) -> CliEnvelope`.

- [ ] **Step 1: Escrever testes do comando exato, do ambiente e do isolamento**

```python
# tests/unit/test_claude_request.py
FORBIDDEN_FLAGS = {"--fallback-model", "--resume", "--continue", "--mcp-config", "--add-dir",
                   "--allowedTools", "--allowed-tools", "--dangerously-skip-permissions", "--agents"}

def test_argv_is_fixed_and_has_no_tools_or_external_context(prepared_request, transport_factory) -> None:
    argv = transport_factory().build_argv(prepared_request)
    assert argv[0] == "/opt/claude-code/bin/claude"
    assert argv[1] == "-p"
    assert pairs(argv)["--model"] == "claude-sonnet-5-5"
    assert pairs(argv)["--effort"] == "low"
    assert pairs(argv)["--output-format"] == "json"
    assert pairs(argv)["--tools"] == ""
    assert pairs(argv)["--setting-sources"] == ""
    assert json.loads(pairs(argv)["--json-schema"]) == canonical_claude_schema_v1()
    assert Path(pairs(argv)["--system-prompt-file"]).is_relative_to(prepared_request.resource_directory)
    assert {"--strict-mcp-config", "--disable-slash-commands", "--no-session-persistence"} <= set(argv)
    assert Decimal(pairs(argv)["--max-budget-usd"]) == prepared_request.max_budget_usd_per_call
    assert not FORBIDDEN_FLAGS & set(argv)
    assert prepared_request.sealed_json not in " ".join(argv)  # pacote só via stdin

def test_environment_is_minimal_and_carries_exactly_one_credential(prepared_request, transport_factory, secrets) -> None:
    env = transport_factory(credential_kind="api_key").build_env(secrets)
    assert set(env) == {"PATH", "HOME", "ANTHROPIC_API_KEY", "DISABLE_AUTOUPDATER",
                        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "CLAUDE_CODE_MAX_OUTPUT_TOKENS"}
    assert env["CLAUDE_CODE_MAX_OUTPUT_TOKENS"] == "4000"
    assert env["HOME"] == "/var/lib/agente-analista/claude-home"
    oauth_env = transport_factory(credential_kind="oauth_token").build_env(secrets)
    assert "CLAUDE_CODE_OAUTH_TOKEN" in oauth_env and "ANTHROPIC_API_KEY" not in oauth_env

def test_combined_prompt_input_and_schema_over_limit_fails_before_subprocess(oversized_sealed_input, request_builder, spawn_recorder) -> None:
    with pytest.raises(RequestPayloadTooLarge):
        request_builder.prepare(oversized_sealed_input)
    assert spawn_recorder.call_count == 0

# tests/integration/test_claude_subprocess_boundary.py  (usa tests/fixtures/fake_claude_cli.py como binário)
@pytest.mark.asyncio
async def test_stdin_carries_sealed_package_and_cwd_is_empty_workdir(fake_cli_transport, prepared_request) -> None:
    envelope = await fake_cli_transport.analyze(prepared_request, uuid4())
    seen = fake_cli_transport.fake.last_invocation
    assert seen.stdin == prepared_request.stdin_bytes
    assert seen.cwd == "/var/lib/agente-analista/claude-empty" or seen.cwd == str(fake_cli_transport.workdir)
    assert seen.cwd_listing == []
    assert envelope.subtype == "success"

@pytest.mark.asyncio
async def test_timeout_kills_whole_process_group_and_returns_timeout(fake_cli_transport, prepared_request) -> None:
    fake_cli_transport.fake.hang_with_child_process()
    envelope = await fake_cli_transport.analyze(prepared_request, uuid4(), timeout_seconds=1)
    assert envelope.timed_out is True
    assert fake_cli_transport.fake.no_surviving_processes()

@pytest.mark.asyncio
async def test_each_physical_attempt_rereads_rotated_credential(fake_cli_transport, prepared_request, rotating_secret_provider) -> None:
    rotating_secret_provider.value = "first"
    await fake_cli_transport.analyze(prepared_request, uuid4())
    rotating_secret_provider.value = "rotated"
    await fake_cli_transport.analyze(prepared_request, uuid4())
    assert fake_cli_transport.fake.credentials_seen == ["first", "rotated"]

@pytest.mark.asyncio
async def test_stderr_is_truncated_and_redacted_never_logged_raw(fake_cli_transport, prepared_request, caplog) -> None:
    fake_cli_transport.fake.write_stderr("token sk-ant-xyz leaked " * 1000)
    envelope = await fake_cli_transport.analyze(prepared_request, uuid4())
    assert len(envelope.stderr_excerpt) <= 2048 and "sk-ant-xyz" not in envelope.stderr_excerpt
    assert "sk-ant-xyz" not in caplog.text

def test_cli_version_mismatch_blocks_before_any_analysis(transport_factory, fake_cli_reporting_version) -> None:
    fake_cli_reporting_version("9.9.9 (Claude Code)")
    with pytest.raises(ClaudeCliVersionMismatch):
        transport_factory().preflight()
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_claude_request.py tests/integration/test_claude_subprocess_boundary.py -v`

Expected: FAIL por adapter ausente.

- [ ] **Step 3: Implementar o transporte por subprocesso**

```python
# src/statistical_analyst/ai/claude_client.py
class ClaudeCodeTransport:
    def build_argv(self, prepared: PreparedClaudeRequest) -> list[str]:
        return [
            str(self._cfg.claude_cli_path), "-p",
            "--model", FIXED_MODEL,
            "--effort", "low",
            "--output-format", "json",
            "--json-schema", prepared.schema_json,
            "--system-prompt-file", str(prepared.system_prompt_path),
            "--tools", "",
            "--strict-mcp-config",
            "--setting-sources", "",
            "--disable-slash-commands",
            "--no-session-persistence",
            "--max-budget-usd", str(prepared.max_budget_usd_per_call),
        ]

    async def analyze(self, prepared: PreparedClaudeRequest, client_request_id: UUID,
                      timeout_seconds: float = 60.0) -> CliEnvelope:
        env = self.build_env(self._secrets)            # relê a credencial a cada tentativa física
        proc = await asyncio.create_subprocess_exec(
            *self.build_argv(prepared),
            stdin=PIPE, stdout=PIPE, stderr=PIPE,
            cwd=self._cfg.claude_workdir, env=env, start_new_session=True,
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(prepared.stdin_bytes), timeout_seconds)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            kill_process_group(proc.pid)               # SIGTERM, 2 s, depois SIGKILL no grupo
            await proc.wait()
            if isinstance(sys.exc_info()[1], asyncio.CancelledError):
                raise
            return CliEnvelope.timeout(client_request_id)
        return parse_cli_envelope(out, proc.returncode, stderr_excerpt=redact_truncate(err, 2048),
                                  client_request_id=client_request_id)
```

`RequestBuilder` carrega só o prompt do `resource_directory` absoluto e obtém o schema em memória de `canonical_claude_schema_v1()`/`AnalysisResponse`; nunca lê a fixture de testes. `stdin_bytes` é o JSON selado canônico, precedido de uma linha fixa de instrução versionada. Mede-se o total de bytes UTF-8 de prompt de sistema + `stdin_bytes` + schema e reprova-se acima de 16.000. Esse guard roda antes do breaker, da reserva e do subprocesso.

Regras do transporte:

- `preflight()`, chamado na partida do serviço e a cada hora, executa `claude --version` com o mesmo ambiente mínimo e compara com `claude_cli_version`; também confirma que `claude_workdir` existe e está vazio. Qualquer falha abre o breaker `configuration`.
- O `HOME` dedicado (`claude_home`) é o único lugar onde o CLI pode gravar estado próprio. Ele não contém `CLAUDE.md`, `settings.json`, agentes, skills nem servidores MCP; o verificador (Task 18) confirma isso.
- O `stdout` é lido inteiro, mas o parser recusa mais de 64 KiB.
- O `stderr` nunca é registrado bruto: só um trecho de até 2 KiB, redigido (chaves `sk-ant-…`, tokens OAuth, e-mails), entra em `analytics.sqlite`.
- Exceções de `create_subprocess_exec` (binário ausente, permissão) viram `CliEnvelope` de classe `configuration`, sem propagar o ambiente nem a credencial.

`tests/fixtures/fake_claude_cli.py` é um executável Python que imita a versão fixada: lê `stdin`, registra argv/cwd/env em um arquivo do `tmp_path` (com a credencial em hash), devolve as fixtures da Task 12 e simula travamento com processo filho, `stderr` grande e códigos de saída de erro.

- [ ] **Step 4: Executar boundary test sem rede**

Run: `.venv/bin/python -m pytest tests/unit/test_claude_request.py tests/integration/test_claude_subprocess_boundary.py -v`

Expected: PASS com o CLI falso. O teste comprova:

- guard real de 16.000 bytes;
- argv fixo sem ferramentas e ambiente mínimo;
- pacote só via `stdin`;
- CWD vazio;
- timeout de 60 s matando o grupo de processos;
- releitura da credencial a cada tentativa;
- `stderr` redigido.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/ai/request_builder.py src/statistical_analyst/ai/claude_client.py tests/unit/test_claude_request.py tests/integration/test_claude_subprocess_boundary.py tests/fixtures/fake_claude_cli.py
git commit -m "feat: call fixed Claude model through headless Claude Code"
```

### Task 15: Coordenador de duas análises sob deadline global

**Files:**
- Create: `src/statistical_analyst/app.py`
- Modify: `src/statistical_analyst/persistence/analysis_store.py`
- Create: `tests/unit/test_analysis_coordinator.py`
- Create: `tests/integration/test_batch_end_to_end.py`

**Interfaces:**
- Consumes: snapshot, package builder, stores, budget, breaker, transport e validator.
- Produces: `AnalysisCoordinator.run_batch(key) -> BatchResult` com duas `SectionResult` e fallback isolado.

- [ ] **Step 1: Escrever testes de independência e deadline**

```python
# tests/unit/test_analysis_coordinator.py
@pytest.mark.asyncio
async def test_one_reservoir_failure_falls_back_only_that_section(coordinator, transport) -> None:
    transport.result_for["0051"] = valid_envelope(reservoir="0051")
    transport.result_for["0022"] = TimeoutError()
    result = await coordinator.run_batch(WINDOW_KEY)
    assert result.sections["0051"].source == "claude"
    assert result.sections["0022"].source == "local_fallback"
    assert transport.max_concurrency_seen == 2

@pytest.mark.asyncio
async def test_insufficient_window_skips_api(coordinator, transport) -> None:
    coordinator.snapshot = insufficient_snapshot("0051")
    result = await coordinator.run_batch(WINDOW_KEY)
    assert result.sections["0051"].fallback_reason == "CURRENT_WINDOW_INSUFFICIENT"
    assert "0051" not in transport.calls

@pytest.mark.asyncio
async def test_global_deadline_cancels_pending_calls(coordinator_with_slow_transport) -> None:
    result = await coordinator_with_slow_transport.run_batch(WINDOW_KEY, deadline_seconds=180)
    assert all(section.source == "local_fallback" for section in result.sections.values())
    assert coordinator_with_slow_transport.all_spawned_tasks_done()
    assert coordinator_with_slow_transport.usage.unknown_reservations() == 2
    writes_at_return = coordinator_with_slow_transport.store.write_count
    coordinator_with_slow_transport.transport.release_blocked_responses()
    await asyncio.sleep(0)
    assert coordinator_with_slow_transport.store.write_count == writes_at_return

@pytest.mark.asyncio
async def test_snapshot_deadline_returns_two_fallbacks_without_subprocess(coordinator, reader, transport, budget) -> None:
    reader.error = SnapshotDeadlineExceeded()
    result = await coordinator.run_batch(WINDOW_KEY)
    assert {section.fallback_reason for section in result.sections.values()} == {"OPERATIONAL_SNAPSHOT_UNAVAILABLE"}
    assert transport.calls == []
    assert budget.reservation_count == 0

@pytest.mark.asyncio
async def test_oversized_prepared_request_falls_back_before_reservation(coordinator, request_builder, transport, budget) -> None:
    request_builder.error_for["0051"] = RequestPayloadTooLarge(16_001)
    result = await coordinator.run_batch(WINDOW_KEY)
    assert result.sections["0051"].fallback_reason == "CLAUDE_REQUEST_TOO_LARGE"
    assert "0051" not in transport.calls
    assert budget.reservations_for("0051") == 0
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_analysis_coordinator.py tests/integration/test_batch_end_to_end.py -v`

Expected: FAIL por coordinator ausente.

- [ ] **Step 3: Implementar pipeline e concorrência limitada**

```python
async def run_batch(self, key: WindowKey, deadline_seconds: float = 180.0) -> BatchResult:
    deadline = self._clock.monotonic() + deadline_seconds
    batch = self._store.get_or_create_batch(key)
    try:
        snapshot = await asyncio.to_thread(
            self._reader.capture,
            key,
            deadline_monotonic=min(deadline, self._clock.monotonic() + 10),
        )
    except (SnapshotDeadlineExceeded, OperationalContractError, sqlite3.Error) as error:
        return self._snapshot_failure_result(batch, error, reservoirs=("0051", "0022"))
    sealed = {rid: self._load_or_build_and_seal(batch, rid, snapshot) for rid in ("0051", "0022")}
    prepared = {}
    early_fallbacks = {}
    for rid in ("0051", "0022"):
        try:
            prepared[rid] = self._request_builder.prepare(sealed[rid])
        except RequestPayloadTooLarge:
            early_fallbacks[rid] = self._fallback(sealed[rid], "CLAUDE_REQUEST_TOO_LARGE")
    tasks = {
        rid: asyncio.create_task(self._analyze_or_fallback(sealed[rid], prepared[rid], deadline))
        for rid in prepared
    }
    if not tasks:
        return BatchResult(sections=early_fallbacks)
    done, pending = await asyncio.wait(
        tasks.values(), timeout=max(0.0, deadline - self._clock.monotonic())
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    sections = {**early_fallbacks, **{
        rid: task.result() if task in done else self._deadline_fallback(sealed[rid])
        for rid, task in tasks.items()
    }}
    return BatchResult(sections=sections)
```

`_snapshot_failure_result` persiste o motivo seguro no batch e produz as duas seções locais sem pacote nem subprocesso Claude; o renderizador ainda cria um único relatório de indisponibilidade operacional. Somente exceções esperadas de leitura/contrato viram esse fallback; `CancelledError` e falhas fatais de persistência preservam semântica de shutdown/fatal do serviço.

Cada tentativa: request já validado, breaker allow, reserva, chamada, persistir response/usage, reconcile, validar. Retry transitório usa nova reserva. Output model diferente do solicitado é registrado e tratado como falha de contrato. Toda exceção de API vira `SectionResult` local; nunca escapa para o MQTT Core. Ao cancelar por deadline, reconciliar sem usage (mantendo 5000/4000), aguardar todas as tasks com `gather(return_exceptions=True)` e somente então retornar; nenhuma coroutine pode gravar depois do `BatchResult`.

- [ ] **Step 4: Executar testes de restart em cada checkpoint**

Run: `.venv/bin/python -m pytest tests/unit/test_analysis_coordinator.py tests/integration/test_batch_end_to_end.py -v`

Expected: PASS para falha de snapshot, limite de request, falha antes/depois de selar, antes/depois de resposta e antes de report; restart reutiliza pacote, cancelamentos estão concluídos e budgets nunca são liberados sem usage confiável.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/app.py src/statistical_analyst/persistence/analysis_store.py tests/unit/test_analysis_coordinator.py tests/integration/test_batch_end_to_end.py
git commit -m "feat: coordinate independent reservoir interpretations"
```

### Task 16: Fallback local e renderização consultiva segura

**Files:**
- Create: `src/statistical_analyst/domain/report.py`
- Create: `src/statistical_analyst/reporting/fallback.py`
- Create: `src/statistical_analyst/reporting/renderer.py`
- Create: `tests/unit/test_local_fallback.py`
- Create: `tests/unit/test_report_renderer.py`

**Interfaces:**
- Consumes: `BatchResult`, métricas locais seladas, alarmes locais ativos, catálogo de action codes e usage consolidado.
- Produces: `LocalFallback.build(...) -> SectionResult` e `ReportRenderer.render(...) -> RenderedReport` com assunto, texto, HTML, fingerprint e `Message-ID` determinísticos.

- [ ] **Step 1: Escrever testes de precedência visual e autoridade local**

```python
# tests/unit/test_report_renderer.py
def test_one_email_has_local_alarms_before_both_sections(renderer, batch_result) -> None:
    report = renderer.render(
        batch_result,
        active_alarms=[critical_alarm("0051", "DRY_RISK")],
        recipients=("operator@example.invalid",),
    )
    assert report.subject == "[ANÁLISE IA 4H] Reservatórios 0051 e 0022 — 2026-08-12 04:00"
    assert report.text.index("ALARMES OPERACIONAIS ATIVOS") < report.text.index("Reservatório 0051")
    assert report.text.index("Reservatório 0051") < report.text.index("Reservatório 0022")
    assert "ANÁLISE CONSULTIVA POR IA — NÃO É COMANDO OPERACIONAL" in report.text
    assert report.priority == local_report_priority(batch_result)

def test_llm_cannot_change_subject_recipients_priority_or_numbers(renderer, interpreted_batch) -> None:
    report = renderer.render(interpreted_batch, active_alarms=[], recipients=RECIPIENTS)
    assert report.recipients == RECIPIENTS
    assert "1,047" not in report.text  # número narrativo não fornecido nunca aparece
    assert "urgente" not in report.subject.lower()

# tests/unit/test_local_fallback.py
def test_fallback_is_deterministic_and_uses_only_catalog_codes(package, resource_directory) -> None:
    first = build_fallback(package, reason="CLAUDE_TIMEOUT", resource_directory=resource_directory)
    second = build_fallback(package, reason="CLAUDE_TIMEOUT", resource_directory=resource_directory)
    assert first == second
    assert set(first.action_codes) <= load_action_catalog(resource_directory).keys()
```

- [ ] **Step 2: Executar os testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_local_fallback.py tests/unit/test_report_renderer.py -v`

Expected: FAIL porque fallback e renderer não existem.

- [ ] **Step 3: Implementar fallback e composição sem delegar decisões à LLM**

```python
# src/statistical_analyst/reporting/renderer.py
class ReportRenderer:
    def render(self, batch: BatchResult, active_alarms: Sequence[LocalAlarm],
               recipients: tuple[str, ...]) -> RenderedReport:
        subject = fixed_subject(batch.window_end_local)
        sections = tuple(self._render_section(batch.sections[rid]) for rid in ("0051", "0022"))
        text = self._text_template.render(
            banner=CONSULTATIVE_BANNER,
            window=batch.window,
            alarms=render_local_alarms(active_alarms),
            sections=sections,
            usage=batch.local_usage_summary,
        )
        return RenderedReport(
            subject=subject,
            recipients=recipients,
            priority=local_report_priority(batch),
            text=text,
            html=self._html_from_same_view_model(batch, active_alarms, sections),
            fingerprint=report_fingerprint(batch.window_key),
            rfc_message_id=stable_message_id(batch.window_key),
        )
```

O view model numérico é reconstruído dos pacotes locais, nunca do texto da resposta. Resolver `evidence_refs` contra o pacote e `action_code` contra o catálogo antes de renderizar. Alarmes mantêm severidade local e aparecem primeiro. Uma seção fallback e outra OpenAI continuam no mesmo e-mail. Escapar todo texto no HTML; não inserir HTML vindo da LLM.

- [ ] **Step 4: Executar regressão dos templates em texto e HTML**

Run: `.venv/bin/python -m pytest tests/unit/test_local_fallback.py tests/unit/test_report_renderer.py -v`

Expected: PASS; snapshots de texto e HTML são estáveis e nenhuma saída da LLM altera assunto, destinatários, prioridade, cadência ou números locais.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/domain/report.py src/statistical_analyst/reporting/fallback.py src/statistical_analyst/reporting/renderer.py tests/unit/test_local_fallback.py tests/unit/test_report_renderer.py
git commit -m "feat: render safe consultative analysis reports"
```

### Task 17: Outbox analítica e entrega Gmail independente

**Files:**
- Modify: `src/statistical_analyst/persistence/analysis_store.py`
- Modify: `src/statistical_analyst/persistence/outbox_store.py`
- Create: `src/statistical_analyst/reporting/gmail_transport.py`
- Create: `src/statistical_analyst/reporting/dispatcher.py`
- Create: `tests/integration/test_report_outbox.py`
- Create: `tests/integration/test_analytics_gmail.py`

**Interfaces:**
- Consumes: `RenderedReport`, configuração Gmail e `SecretProvider` da Task 1.
- Produces: transação `save_report_and_enqueue`, `ReportOutbox` com claim/lease e `AnalyticsDispatcher.run_once(now)`.

- [ ] **Step 1: Escrever testes de atomicidade, lease e recuperação SMTP**

```python
# tests/integration/test_report_outbox.py
def test_report_and_outbox_are_atomic_and_unique(store, rendered_report) -> None:
    store.save_report_and_enqueue(rendered_report)
    store.save_report_and_enqueue(rendered_report)
    assert store.count_reports(rendered_report.fingerprint) == 1
    assert store.outbox.count(rendered_report.fingerprint) == 1

def test_expired_claim_is_recovered_with_same_rfc_message_id(outbox, rendered_report, now) -> None:
    outbox.enqueue(rendered_report)
    first = outbox.claim_due(now, worker_id=uuid4())
    recovered = outbox.claim_due(now + minutes(6), worker_id=uuid4())
    assert recovered.outbox_id == first.outbox_id
    assert recovered.rfc_message_id == first.rfc_message_id == rendered_report.rfc_message_id

# tests/integration/test_analytics_gmail.py
def test_starttls_certificate_validation_and_retry(fake_smtp, dispatcher, queued_report, now) -> None:
    fake_smtp.fail_once(TemporarySmtpError(451))
    dispatcher.run_once(now)
    assert queued_report.status == "RETRY"
    dispatcher.run_once(queued_report.next_attempt_at)
    assert queued_report.status == "SENT"
    assert fake_smtp.messages[0]["Message-ID"] == fake_smtp.messages[1]["Message-ID"]

def test_permanent_delivery_moves_its_own_message_to_dead(fake_smtp, dispatcher, queued_report_permanent, now) -> None:
    fake_smtp.fail(PermanentSmtpError(550))
    assert dispatcher.run_once(now).status == "DEAD"
    assert queued_report_permanent.safe_error_code == "SMTP_PERMANENT"

def test_accept_then_disconnect_is_uncertain_without_automatic_retry(
    fake_smtp, dispatcher, queued_report_uncertain, now
) -> None:
    fake_smtp.accept_then_disconnect()
    assert dispatcher.run_once(now).status == "DELIVERY_UNCERTAIN"
    assert queued_report_uncertain.status == "DELIVERY_UNCERTAIN"
    assert dispatcher.run_once(now + hours(1)).status == "IDLE"

def test_gmail_transport_rereads_rotated_app_password(fake_smtp, secret_provider, gmail_transport) -> None:
    secret_provider.value = "first"
    gmail_transport.send(message("one"))
    secret_provider.value = "rotated"
    gmail_transport.send(message("two"))
    assert fake_smtp.login_passwords == ["first", "rotated"]
```

- [ ] **Step 2: Executar os testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/integration/test_report_outbox.py tests/integration/test_analytics_gmail.py -v`

Expected: FAIL por store e transporte ausentes.

- [ ] **Step 3: Implementar outbox própria e política de entrega**

```python
# src/statistical_analyst/reporting/dispatcher.py
def run_once(self, now: datetime) -> DispatchResult:
    message = self._outbox.claim_due(now, self._worker_id, lease=timedelta(minutes=5))
    if message is None:
        return DispatchResult.idle()
    try:
        provider_id = self._transport.send(message.to_email_message())
    except TemporaryMailError as error:
        self._outbox.reschedule(
            message.outbox_id,
            next_attempt_at=now + smtp_backoff(message.attempt_count, maximum=timedelta(minutes=30)),
            error_code=error.safe_code,
        )
        return DispatchResult.retry_scheduled()
    except PermanentMailError as error:
        self._outbox.mark_dead(message.outbox_id, error.safe_code, self._clock.utc_now())
        return DispatchResult.dead()
    except DeliveryUncertainError as error:
        self._outbox.mark_uncertain(message.outbox_id, error.safe_code, self._clock.utc_now())
        return DispatchResult.delivery_uncertain()
    self._outbox.mark_sent(message.outbox_id, provider_id, self._clock.utc_now())
    return DispatchResult.sent()
```

`GmailSmtpTransport` usa `smtplib.SMTP`, `ssl.create_default_context()`, STARTTLS com validação de certificado e chama `SecretProvider.get_required(gmail_credential_name)` em cada tentativa de autenticação; não mantém senha de app em cache. Nunca logar envelope, destinatários, corpo ou segredo. Classifica permanent, temporary e uncertain sem sobreposição: permanentes ficam em `DEAD`; transitórias repetem com backoff crescente limitado a 30 minutos; aceite possivelmente concluído seguido de desconexão fica em `DELIVERY_UNCERTAIN` e requer reconciliação humana, sem retry automático. O writer, a conexão SQLite, a thread e o processo são exclusivamente analíticos.

O contrato garante uma linha lógica por fingerprint e reutiliza o mesmo RFC `Message-ID` em toda tentativa. SMTP não oferece confirmação transacional junto ao SQLite: uma queda depois de o Gmail aceitar e antes de `mark_sent` é `DELIVERY_UNCERTAIN`, deve ficar visível no runbook/telemetria e não pode ser descrita como garantia física de exactly-once.

- [ ] **Step 4: Executar testes de crash/restart e SMTP fake**

Run: `.venv/bin/python -m pytest tests/integration/test_report_outbox.py tests/integration/test_analytics_gmail.py -v`

Expected: PASS; falha antes do envio é repetida, report já confirmado não é reenfileirado e toda repetição mantém fingerprint e `Message-ID`.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/persistence/analysis_store.py src/statistical_analyst/persistence/outbox_store.py src/statistical_analyst/reporting/gmail_transport.py src/statistical_analyst/reporting/dispatcher.py tests/integration/test_report_outbox.py tests/integration/test_analytics_gmail.py
git commit -m "feat: deliver reports through isolated Gmail outbox"
```

### Task 18: Runtime, serviço systemd, observabilidade e proteção local

**Files:**
- Consume unchanged: `resources/contracts/operational_v1.json`
- Consume unchanged: `resources/prompts/statistical_analyst_v1.txt`
- Consume unchanged: `resources/catalogs/action_codes_v1.json`
- Consume unchanged: `resources/catalogs/claude_error_classes_v1.json`
- Create: `src/statistical_analyst/__main__.py`
- Create: `src/statistical_analyst/observability.py`
- Modify: `src/statistical_analyst/security.py`
- Create: `src/statistical_analyst/service/runtime.py`
- Create: `src/statistical_analyst/service/linux_service.py`
- Create: `deploy/systemd/statistical-analyst.service`
- Create: `scripts/install-statistical-analyst.sh`
- Create: `scripts/uninstall-statistical-analyst.sh`
- Create: `scripts/verify-statistical-analyst.sh`
- Create: `tests/unit/test_analyst_runtime.py`
- Create: `tests/unit/test_analyst_log_redaction.py`
- Create: `tests/integration/test_analytics_retention_backup.py`
- Create: `tests/integration/test_operational_readiness.py`
- Create: `tests/integration/test_operational_readonly_permissions.py`
- Create: `tests/operational/test_statistical_analyst_service.py`

**Interfaces:**
- Consumes: config validada, coordinator, scheduler, dispatcher, stop event, `SystemdNotifier` (mesma implementação sem dependências do Core, copiada para `statistical_analyst.service`, sem importar o pacote do Core) e caminhos absolutos Linux.
- Produces: `AnalystRuntime.start/stop`, unidade `statistical-analyst.service`, logs estruturados redigidos no `journald`, retenção/backup de 90 dias e preflight real de implantação.

- [ ] **Step 1: Escrever testes de lifecycle, redaction e retenção**

```python
# tests/unit/test_analyst_runtime.py
def test_stop_cancels_analysis_and_drains_completed_outbox(runtime, fake_components) -> None:
    runtime.start()
    runtime.stop(grace_seconds=30)
    assert fake_components.scheduler.stopped
    assert fake_components.claude.cancelled
    assert fake_components.analytics_writer.closed
    assert fake_components.core_components_touched == 0

def test_runtime_waits_for_core_contract_readiness_before_scheduler(runtime, fake_components) -> None:
    fake_components.operational_readiness.sequence = ["MISSING", "NOT_READY", "READY"]
    runtime.start()
    assert fake_components.order.index("operational.ready") < fake_components.order.index("scheduler.start")

# tests/integration/test_operational_readiness.py
def test_unknown_contract_never_migrates_core_or_starts_analysis(readiness_harness) -> None:
    readiness_harness.publish_contract(version="operational-v2", ready=1)
    with pytest.raises(UnsupportedOperationalContract):
        readiness_harness.wait(timeout_seconds=2)
    assert readiness_harness.operational_write_attempts == 0

def test_runtime_loads_only_installed_resources_outside_repo(runtime, fake_components, tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    runtime.start()
    assert fake_components.loaded_resource_paths == {
        runtime.config.resource_directory / "contracts" / "operational_v1.json",
        runtime.config.resource_directory / "prompts" / "statistical_analyst_v1.txt",
        runtime.config.resource_directory / "catalogs" / "action_codes_v1.json",
    }

# tests/unit/test_analyst_log_redaction.py
def test_logs_never_emit_prompts_responses_recipients_or_secrets(caplog, redacting_logger) -> None:
    redacting_logger.analysis_failed(api_key="sk-ant-secret", recipient="x@gmail.com", response='{"summary":"raw"}')
    assert "sk-ant-secret" not in caplog.text
    assert "x@gmail.com" not in caplog.text
    assert '"summary":"raw"' not in caplog.text

# tests/integration/test_analytics_retention_backup.py
def test_cleanup_keeps_last_ninety_days_and_backup_is_consistent(analytics_db, now, backup_path) -> None:
    seed_analytics(analytics_db, ages_days=[89, 90, 91])
    result = cleanup_and_backup(analytics_db, backup_path, now)
    assert result.deleted_older_than_cutoff == 1
    assert integrity_check(backup_path) == "ok"
```

```python
# tests/integration/test_operational_readonly_permissions.py
def test_analyst_group_can_read_wal_database_but_never_write(core_db_as_other_user, analyst_identity) -> None:
    conn = analyst_identity.connect(f"file:{core_db_as_other_user}?mode=ro", uri=True)
    conn.execute("PRAGMA query_only=ON")
    assert conn.execute("SELECT count(*) FROM raw_events").fetchone()[0] >= 0
    with pytest.raises(sqlite3.OperationalError):
        conn.execute("UPDATE raw_events SET topic=topic")
    assert analyst_identity.cannot_create_file_in(core_db_as_other_user.parent)
```

- [ ] **Step 2: Executar os testes e confirmar a falha**

Run: `.venv/bin/python -m pytest tests/unit/test_analyst_runtime.py tests/unit/test_analyst_log_redaction.py tests/integration/test_analytics_retention_backup.py tests/integration/test_operational_readiness.py tests/integration/test_operational_readonly_permissions.py -v`

Expected: FAIL por runtime e rotinas ausentes.

- [ ] **Step 3: Implementar composição, unidade systemd e preflight seguro**

```python
# src/statistical_analyst/service/runtime.py
class AnalystRuntime:
    def start(self) -> None:
        self._operational_readiness.wait(
            expected_contract="operational-v1",
            expected_manifest_sha256=self._contract.manifest_sha256,
            timeout_seconds=120,
            stop_event=self._stop_event,
        )
        self._analytics_db.migrate()
        self._claude_transport.preflight()   # versão do CLI, workdir vazio, HOME sem configuração
        self._dispatcher.start(self._stop_event)
        self._scheduler.start(self._stop_event)
        self._notifier.notify("READY=1\nSTATUS=scheduler running")

    def stop(self, grace_seconds: float = 30.0) -> None:
        self._notifier.notify("STOPPING=1")
        self._stop_event.set()
        self._coordinator.cancel_pending()   # mata os grupos de processos `claude` ainda vivos
        self._scheduler.join(timeout=grace_seconds)
        self._dispatcher.drain_confirmed(timeout=grace_seconds)
        self._analytics_db.close()
```

`OperationalReadiness` faz o seguinte:

- abre somente `mode=ro`;
- espera, com backoff limitado a 120 segundos por arquivo, até encontrar `operational_contract.ready=1`, `runtime_health.core_ready=1`, versão/hash/rule version conhecidos e o schema exato;
- respeita o stop event e jamais cria ou migra tabela operacional;
- com versão ou hash incompatível, falha imediatamente;
- com o arquivo ausente ou `ready=0`, espera até o timeout e então encerra com código ≠ 0, para o `systemd` reiniciar (`Restart=on-failure`).

`linux_service.py` traduz `SIGTERM`/`SIGINT` para o mesmo stop event e envia `READY=1`/`WATCHDOG=1`/`STOPPING=1`. Ele nunca importa nem inicia o runtime do MQTT Core.

```ini
# deploy/systemd/statistical-analyst.service
[Unit]
Description=Statistical Analyst consultivo (Claude Code headless)
Requires=mqtt-core.service
After=mqtt-core.service network-online.target time-sync.target
Wants=network-online.target

[Service]
Type=notify
NotifyAccess=main
User=agente-analista
Group=agente-analista
SupplementaryGroups=agente-mqtt-ro
WorkingDirectory=/
ExecStart=/opt/agente-mqtt/venv/bin/python -m statistical_analyst run --config /etc/agente-mqtt/statistical-analyst.toml
Restart=on-failure
RestartSec=30
WatchdogSec=300
TimeoutStopSec=40
KillMode=control-group
Nice=10
IOSchedulingClass=idle
CPUWeight=20
MemoryMax=1G
TasksMax=64
LoadCredential=claude.credential:/etc/credstore/agente-analista/claude.credential
LoadCredential=gmail.username:/etc/credstore/agente-analista/gmail.username
LoadCredential=gmail.app_password:/etc/credstore/agente-analista/gmail.app_password
UMask=0077
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
PrivateDevices=yes
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectControlGroups=yes
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
RestrictNamespaces=yes
LockPersonality=yes
SystemCallArchitectures=native
CapabilityBoundingSet=
ReadOnlyPaths=/var/lib/agente-mqtt /opt/agente-mqtt /opt/claude-code /etc/agente-mqtt
ReadWritePaths=/var/lib/agente-analista /var/backups/agente-analista /var/log/agente-analista

[Install]
WantedBy=multi-user.target
```

Observações sobre a unidade:

- `MemoryDenyWriteExecute` **não** é usado, porque o Node.js do Claude Code precisa de JIT. Essa é a única diferença de endurecimento em relação ao Core e fica registrada na saída do verificador.
- `Requires=`/`After=` garantem que o Core sobe antes. O `READY` do Analyst só sai depois do contrato `ready=1`.
- `KillMode=control-group` garante que nenhum processo `claude` sobreviva ao stop.

`scripts/install-statistical-analyst.sh` (`set -euo pipefail`, exige `root`, idempotente):

- cria o usuário de sistema `agente-analista` (sem shell) e o adiciona **somente** ao grupo `agente-mqtt-ro`;
- cria os diretórios `/var/lib/agente-analista` (`0700`), `claude-empty` (`0500`, vazio), `claude-home` (`0700`), `/var/backups/agente-analista` e `/var/log/agente-analista`;
- instala o Claude Code CLI na versão `claude_cli_version` em `/opt/claude-code`, com dono `root` e modo `0755`, sem autoatualização. A instalação usa o instalador oficial ou o pacote npm na versão exata, e o hash do binário/pacote resultante é registrado;
- resolve as fontes empacotadas em `<venv>/share/agente-mqtt/contracts` e `<venv>/share/agente-mqtt/statistical-analyst`, verifica os hashes aprovados e copia contrato, prompt e catálogos por arquivo temporário + `mv` para `resource_directory`, com dono `root` e modo `0644`;
- instala a unidade e roda `systemd-analyze verify` e `systemctl daemon-reload`/`enable`;
- antes de alterar qualquer caminho, valida exatamente cada destino e aborta se estiver fora do diretório configurado;
- **não** recebe nem grava segredos: imprime os três caminhos em `/etc/credstore/agente-analista/` a provisionar (modo `0600`, dono `root`) e o valor esperado de `claude_credential_kind`.

O composition root de produção instancia um único `SystemdCredentialProvider` e o injeta no transporte Claude e no Gmail. Teste operacional da credencial:

- provisiona uma credencial descartável e a lê pelo processo do serviço;
- rotaciona o valor, reinicia o serviço, executa nova tentativa com o CLI falso (sem rede) e comprova que o novo valor foi usado;
- remove a credencial e confirma falha segura, sem revelar o conteúdo.

`ANTHROPIC_API_KEY` no ambiente do shell só é aceita pelo harness live interativo, nunca pela unidade `systemd`.

`verify-statistical-analyst.sh` reprova a implantação em qualquer um destes casos:

- o banco operacional não abre como `agente-analista` em `mode=ro`, ou um `UPDATE`/arquivo sentinela **consegue** ser feito com a identidade real (`runuser -u agente-analista`);
- `analytics.sqlite` é legível ou gravável por outro usuário;
- `claude --version` diverge da configuração;
- `claude-empty` não está vazio;
- `claude-home` contém `settings.json`, `CLAUDE.md`, agentes, skills, plugins ou configuração MCP;
- `systemd-analyze security statistical-analyst.service` passa do limite registrado;
- o volume de dados não está cifrado (LUKS) e falta a flag `--accept-provider-encryption`.

O backup usa `sqlite3.Connection.backup()`, nunca cópia direta de `.sqlite`/WAL, e segue para o mesmo destino externo cifrado do Core. O teste unitário não simula permissões nem cifragem; o teste operacional roda como `root` na VPS de homologação e verifica o estado real.

- [ ] **Step 4: Executar testes portáveis e smoke test na VPS**

Run: `.venv/bin/python -m pytest tests/unit/test_analyst_runtime.py tests/unit/test_analyst_log_redaction.py tests/integration/test_analytics_retention_backup.py tests/integration/test_operational_readiness.py tests/integration/test_operational_readonly_permissions.py -v`

Expected: PASS.

Run como `root` na VPS de homologação: `sudo .venv/bin/python -m pytest tests/operational/test_statistical_analyst_service.py -v --run-operational`

Expected: PASS. O teste comprova que o serviço:

- sobe depois de `mqtt-core.service`, com `Nice=10`, e envia `READY` só após o contrato ready;
- carrega recursos absolutos e credenciais via `LoadCredential`;
- para em até 30 segundos sem processos `claude` remanescentes;
- reinicia por `systemd` e após reboot;
- mantém os dois bancos e as duas outboxes isolados.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/__main__.py src/statistical_analyst/observability.py src/statistical_analyst/security.py src/statistical_analyst/service deploy/systemd/statistical-analyst.service scripts/install-statistical-analyst.sh scripts/uninstall-statistical-analyst.sh scripts/verify-statistical-analyst.sh tests/unit/test_analyst_runtime.py tests/unit/test_analyst_log_redaction.py tests/integration/test_analytics_retention_backup.py tests/integration/test_operational_readiness.py tests/integration/test_operational_readonly_permissions.py tests/operational/test_statistical_analyst_service.py
git commit -m "feat: operate isolated statistical analyst systemd service"
```

### Task 19: Gates dourados, isolamento E2E, testes live e piloto

**Files:**
- Create: `tests/fixtures/golden/normal.json`
- Create: `tests/fixtures/golden/drying.json`
- Create: `tests/fixtures/golden/overflowing.json`
- Create: `tests/fixtures/golden/data_quality_failures.json`
- Create: `tests/fixtures/golden/historical_and_alarm_cases.json`
- Create: `tests/fixtures/golden/claude_failures.json`
- Create: `tests/e2e/test_mqtt_core_isolation.py`
- Create: `tests/e2e/test_retention_snapshot_contract.py`
- Create: `tests/e2e/test_full_analysis_report.py`
- Create: `tests/e2e/conftest.py`
- Create: `tests/e2e/harness.py`
- Create: `tests/live/test_claude_contract.py`
- Create: `tests/live/test_capture_cli_fixtures.py`
- Create: `tests/live/test_fixture_token_counts.py`
- Create: `src/statistical_analyst/pilot/__init__.py`
- Create: `src/statistical_analyst/pilot/evaluator.py`
- Create: `tests/unit/test_pilot_evaluator.py`
- Modify: `tests/conftest.py`
- Create: `scripts/run-statistical-pilot.sh`
- Create: `scripts/evaluate-statistical-pilot.sh`
- Create: `docs/runbooks/statistical-analyst-pilot.md`
- Create: `docs/runbooks/statistical-analyst-operations.md`

**Interfaces:**
- Consumes: sistema completo, broker/SMTP/CLI Claude falsos, credencial Claude opt-in e ficha de avaliação humana.
- Produces: gates automatizados, relatório de piloto e decisão explícita `PROMOTE`, `EXTEND` ou `BLOCK`.

`tests/conftest.py` deve preservar a implementação única criada pelo Core; apenas completar o gating de `--run-live` e `--run-operational` se ainda necessário, sem redefinir opções. `tests/e2e/harness.py` possui `SystemHarness`, `GoldenHarness`, `GOLDEN_SCENARIOS` e builders dos fakes broker/SMTP/CLI Claude; `tests/e2e/conftest.py` expõe essas fixtures. No teste de isolamento, cada runtime é iniciado em subprocesso separado; o processo analítico não importa o composition root do Core.

- [ ] **Step 1: Escrever matriz dourada e teste de isolamento crítico**

```python
# tests/e2e/test_mqtt_core_isolation.py
def test_slow_saturated_analyst_cannot_delay_core_persistence_or_critical_alert(system_harness) -> None:
    system_harness.claude.block_for(seconds=240)
    analysis = system_harness.start_analysis_window()
    event = system_harness.publish("EPZ/META/0051/ANI01/RAW", b"900", qos=1)
    assert system_harness.core.wait_persisted(event, within_seconds=5)
    assert system_harness.core.wait_for_alarm("DRY_RISK", within_spec_deadline=True)
    assert system_harness.core.database_path != analysis.database_path
    assert system_harness.core.outbox_worker_id != analysis.outbox_worker_id

# tests/e2e/test_retention_snapshot_contract.py
from datetime import timedelta
import pytest

def test_core_retention_preserves_carry_consumed_by_analyst(system_harness) -> None:
    window_end = system_harness.current_window_end
    system_harness.core.seed_transition("0051", "quality", "GOOD", age_days=500)
    carry_id = system_harness.core.seed_transition("0051", "quality", "MISSING", age_days=400)
    recent_id = system_harness.core.seed_transition(
        "0051", "quality", "GOOD", effective_at=window_end - timedelta(hours=3)
    )
    system_harness.core.run_retention()
    snapshot = system_harness.analyst.capture_window(window_end)
    assert snapshot.transition_ids("0051", "quality") == [carry_id, recent_id]
    assert snapshot.quality_durations_minutes("0051").total == pytest.approx(240.0, abs=1/60)

# tests/e2e/test_full_analysis_report.py
@pytest.mark.parametrize("fixture_name", GOLDEN_SCENARIOS)
def test_golden_scenario_has_exact_local_result_and_safe_report(fixture_name, harness) -> None:
    expected = load_golden(fixture_name)
    actual = harness.run_fixture(expected.input)
    assert actual.local_facts == expected.local_facts
    assert set(actual.action_codes) <= set(expected.acceptable_action_codes)
    assert actual.safety_violations == []
```

Os fixtures cobrem: normal, secagem, extravasamento, variabilidade, ausência, travamento, inválido, salto `>0.4`, histórico insuficiente, alarme ativo, tentativa de reduzir alarme, timeout do subprocesso, sobrecarga/5xx/rate limit, crédito/cota da assinatura/auth, `error_max_budget_usd`, versão do CLI divergente, tentativa de ferramenta, modelo divergente em `modelUsage`, schema inválido, budgets de tokens e USD esgotados, ausência de usage e bloqueio de segredo/e-mail.

- [ ] **Step 2: Executar suites sem rede e confirmar falhas iniciais**

Run: `.venv/bin/python -m pytest tests/unit tests/integration tests/e2e -m "not live and not operational and not mosquitto and not soak" -v`

Expected: FAIL até fixtures, harness e verificações finais existirem.

- [ ] **Step 3: Implementar testes live opt-in e avaliação do piloto**

```python
# tests/live/test_fixture_token_counts.py
@pytest.mark.live
@pytest.mark.parametrize("fixture", all_request_fixtures())
def test_effective_input_tokens_reported_by_cli_are_within_budget(live_transport, request_builder, fixture) -> None:
    envelope = run(live_transport.analyze(request_builder.prepare(fixture), uuid4()))
    u = envelope.usage
    assert u.input_tokens + u.cache_creation_input_tokens + u.cache_read_input_tokens <= 5000
    assert u.output_tokens <= 4000

# tests/live/test_claude_contract.py
@pytest.mark.live
def test_fixed_model_returns_valid_structured_output_without_tools(live_transport, request_builder, sealed_normal_input) -> None:
    prepared = request_builder.prepare(sealed_normal_input)
    envelope = run(live_transport.analyze(prepared, uuid4()))
    assert envelope.subtype == "success" and envelope.is_error is False
    assert set(envelope.model_usage) == {"claude-sonnet-5-5"}
    assert envelope.tool_use_count == 0 and envelope.permission_denials == []
    assert validate_response(envelope, sealed_normal_input).is_valid

@pytest.mark.live
def test_prompt_injection_asking_for_file_read_cannot_use_tools(live_transport, request_builder, sealed_injection_input) -> None:
    envelope = run(live_transport.analyze(request_builder.prepare(sealed_injection_input), uuid4()))
    assert envelope.tool_use_count == 0 and envelope.permission_denials == []
    assert "/etc/" not in json.dumps(envelope.structured_output or {})

# tests/live/test_capture_cli_fixtures.py
@pytest.mark.live
def test_capture_success_and_error_envelopes_for_pinned_cli(live_transport, capture_dir) -> None:
    capture = capture_cli_envelopes(live_transport, cases=["success", "invalid_credential", "tiny_budget"])
    assert capture.cli_version == live_transport.config.claude_cli_version
    write_redacted_fixtures(capture, capture_dir)   # revisão humana antes de copiar para tests/fixtures/claude_cli
```

```python
# tests/unit/test_pilot_evaluator.py
def test_timeliness_boundary_is_forty_of_forty_two() -> None:
    assert evaluate_pilot(pilot_record(scheduled=42, timely=39)).gate("email_timeliness").passed is False
    assert evaluate_pilot(pilot_record(scheduled=42, timely=40)).gate("email_timeliness").passed is True

def test_valid_section_sample_extends_at_day_seven_and_blocks_at_day_fourteen() -> None:
    assert evaluate_pilot(pilot_record(days=7, valid_sections=59)).decision == "EXTEND"
    assert evaluate_pilot(pilot_record(days=14, valid_sections=59)).decision == "BLOCK"
    assert evaluate_pilot(pilot_record(days=7, valid_sections=60, all_gates_pass=True)).decision == "PROMOTE"

def test_human_score_and_finding_denominators_use_exact_boundaries() -> None:
    result = evaluate_pilot(pilot_record(
        valid_sections=60, technically_correct=54, useful_at_least_four=48,
        clear_at_least_four=51, findings=20, unsupported_findings=1,
    ))
    assert result.gate("technical_correctness").ratio == Fraction(54, 60)
    assert result.gate("unsupported_findings").ratio == Fraction(1, 20)
    assert result.gate("usefulness").passed and result.gate("clarity").passed

@pytest.mark.parametrize("mutation", [
    {"safety_violations": 1}, {"pending_disagreements": 1},
    {"data_processing_approved": False}, {"missing_gate_evidence": ["budget_compliance"]},
])
def test_safety_disagreement_data_approval_or_missing_evidence_prevents_promotion(mutation) -> None:
    assert evaluate_pilot(pilot_record(all_gates_pass=True, **mutation)).decision != "PROMOTE"

def test_evaluator_always_reports_all_fourteen_named_gates() -> None:
    result = evaluate_pilot(pilot_record(all_gates_pass=True))
    assert len(result.gates) == 14
    assert set(result.gates) == set(APPROVED_GATE_NAMES)
```

`PilotEvaluator` usa inteiros/Fraction, persiste numerador, denominador, evidência e estado de cada um dos 14 critérios; nunca arredonda percentuais para aprovar. Para zero achados, o gate de não sustentados só passa se o numerador também for zero e registra denominador zero explicitamente. Gate ausente, revisão humana incompleta ou discordância pendente nunca promove. Violação de segurança, privacidade, orçamento ou isolamento gera `BLOCK` imediato; amostra/nota ainda incompleta gera `EXTEND` no dia 7 e `BLOCK` no dia 14; `PROMOTE` exige ao menos 7 dias, 42 execuções, 60 seções válidas, aprovação de processamento de dados e todos os 14 gates aprovados.

Os testes `live` exigem ao mesmo tempo `--run-live`, a credencial dedicada (`ANTHROPIC_API_KEY` do workspace do agente ou `CLAUDE_CODE_OAUTH_TOKEN` do piloto) e `CLAUDE_LIVE_COST_ACCEPTED=YES`, e ficam fora do CI normal. Eles usam o binário real em `claude_cli_path`, com a mesma argv e o mesmo ambiente mínimo da produção. Custo (`total_cost_usd`), `session_id` e contagens são registrados à parte, sem guardar conteúdo em logs. As fixtures capturadas da versão fixada substituem as da Task 12 só depois de revisão humana. `run-statistical-pilot.sh` coleta continuamente desde a ativação, sem parar em 42 execuções quando a decisão for `EXTEND`, e encerra somente em `PROMOTE`, `BLOCK` ou 14 dias. `evaluate-statistical-pilot.sh` chama o mesmo `PilotEvaluator` Python, não reimplementa fórmulas em shell, e emite relatório assinado com os 14 gates e denominadores.

O runbook de operações cobre: instalação do wheel sem checkout; instalação e atualização controlada do Claude Code CLI (nova versão só depois de recapturar as fixtures, rodar os testes live e aprovar); paths/hashes dos recursos; provisionamento das credenciais em `/etc/credstore/agente-analista/`; rotação (arquivo + `systemctl restart`) e revogação, inclusive revogar a chave no Console da Anthropic ou o token OAuth; limite de gasto do workspace; dependência/ready do Core, recuperação de `DELIVERY_UNCERTAIN`, circuit breaker/budget, restore do banco analítico e rollback que preserva a versão anterior de recursos. Todos os comandos de verificação devem funcionar com CWD fora do repositório.

- [ ] **Step 4: Executar todos os gates apropriados**

Run: `.venv/bin/python -m pytest tests/unit tests/integration tests/e2e -m "not live and not operational and not mosquitto and not soak" -v --cov=agente_mqtt --cov=statistical_analyst --cov-report=term-missing --cov-fail-under=90`

Expected: PASS; nenhuma chamada de rede real.

Run autorizado antes do piloto: `.venv/bin/python -m pytest tests/live -v --run-live`

Expected: PASS para acesso ao modelo fixo, saída estruturada válida, nenhuma ferramenta usada mesmo com prompt injection, e todas as fixtures com entrada efetiva ≤ 5.000 tokens.

Run como `root` na VPS de piloto: `sudo scripts/verify-statistical-analyst.sh --config /etc/agente-mqtt/statistical-analyst.toml`

Expected: PASS para usuário, permissões, cifragem, paths, versão do CLI, `HOME`/workdir limpos, serviço, acesso read-only e bancos separados.

- [ ] **Step 5: Commit**

```bash
git add src/statistical_analyst/pilot tests/unit/test_pilot_evaluator.py tests/conftest.py tests/fixtures/golden tests/e2e tests/live scripts/run-statistical-pilot.sh scripts/evaluate-statistical-pilot.sh docs/runbooks/statistical-analyst-pilot.md docs/runbooks/statistical-analyst-operations.md
git commit -m "test: gate statistical analyst pilot"
```

## Verificação final deste plano

- [ ] Executar `.venv/bin/python -m pytest tests/unit tests/integration tests/e2e -m "not live and not operational and not mosquitto and not soak" -v --cov=agente_mqtt --cov=statistical_analyst --cov-report=term-missing --cov-fail-under=90`.
- [ ] Executar `.venv/bin/python -m compileall -q src tests`.
- [ ] Executar `.venv/bin/python -m pip check` no ambiente criado pelos lockfiles.
- [ ] Executar `rg -n "TODO|FIXME|pass$|NotImplemented|example\.invalid|sk-" src config scripts deploy docs/runbooks` e resolver qualquer ocorrência executável ou segredo.
- [ ] Verificar que `operational.sqlite` abre com `mode=ro`, `query_only=ON` e que `UPDATE` falha no teste de integração.
- [ ] Inspecionar a invocação capturada pelo CLI falso: argv fixo (`--model claude-sonnet-5-5`, `--effort low`, `--tools ""`, `--strict-mcp-config`, `--setting-sources ""`, `--no-session-persistence`, `--json-schema`, `--max-budget-usd`), nenhuma flag proibida, pacote só via `stdin`, CWD vazio, ambiente mínimo, timeout e retry únicos.
- [ ] Autorizar o custo e executar `tests/live` somente com a credencial dedicada antes do piloto; revisar e versionar as fixtures do CLI capturadas.
- [ ] Executar `sudo scripts/verify-statistical-analyst.sh` na VPS real e anexar a saída ao registro de implantação.
- [ ] Rodar o piloto de 7 dias, ou estender até 14 dias conforme a regra de amostra, e obter decisão técnica explícita antes de ativar produção.
