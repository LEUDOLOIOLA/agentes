# Statistical Analyst OpenAI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir um segundo serviço Windows que, a cada 4 horas, calcula estatísticas locais independentes dos reservatórios `0051` e `0022`, solicita interpretação estruturada ao `gpt-5.6-luna` e entrega um único relatório consultivo pelo Gmail com fallback determinístico.

**Architecture:** `statistical_analyst` abre `operational.sqlite` somente para leitura e captura um snapshot consistente sem compartilhar workers, locks ou outbox com o MQTT Core. Métricas, suficiência, prioridade e alarmes são locais; duas chamadas OpenAI independentes interpretam pacotes selados, e um validador estrito rejeita qualquer saída incompleta, sem evidência ou com linguagem de controle. Estado analítico, orçamento, circuit breaker, relatórios e outbox residem em `analytics.sqlite` próprio.

**Tech Stack:** CPython 3.12 x64, biblioteca padrão, `openai==3.0.0`, `pydantic==2.13.4`, `pywin32==312`, `tzdata==2026.3`, `pytest==9.1.1`, `pytest-cov==7.1.0`, `pytest-asyncio==1.4.0`.

## Global Constraints

- Pré-requisito: concluir o plano `2026-08-12-mqtt-core-implementation-plan.md`; este serviço consome o contrato SQL criado ali e nunca migra `operational.sqlite`.
- Processo, conta Windows, prioridade, banco, writer, outbox, dispatcher Gmail e logs são distintos do MQTT Core.
- Modelo fixo `gpt-5.6-luna`, Responses API, reasoning `low`, `store=false`, background desabilitado, truncation desabilitada, sem tools, memória ou `previous_response_id`.
- Horários: `00:00`, `04:00`, `08:00`, `12:00`, `16:00`, `20:00` em `America/Fortaleza`; snapshot começa em `window_end + 2 minutos`.
- Uma execução analítica global ativa; deadline total de 3 minutos, `busy_timeout=2s`, leitura operacional total de no máximo 10 segundos.
- Um pacote selado por reservatório e janela; bytes canônicos, hash e manifesto persistidos antes de qualquer HTTP.
- Janela atual `[end-4h,end)`, referência não sobreposta `[end-28h,end-4h)` e sete janelas de 4 horas no mesmo horário local dos dias anteriores.
- Estatísticas, tendência, ETA, variabilidade, status, confiança, prioridade, alarmes e números do e-mail são calculados localmente.
- Duas chamadas independentes, concorrência máxima 2; falha de uma afeta somente a seção correspondente.
- Payload completo máximo 16.000 bytes UTF-8 e fixtures verificadas em até 5.000 tokens de entrada; `max_output_tokens=4000`; JSON visível máximo 6.000 bytes.
- Máximo 24 tentativas, 120.000 tokens de entrada reservados e 96.000 de saída em qualquer janela móvel de 24 horas.
- Uma repetição por reservatório somente para rede, 408, 409, 5xx ou 429 temporário; cada tentativa física exige nova reserva.
- `incomplete`, refusal, erro semântico, schema inválido ou conteúdo inesperado sempre geram fallback e nunca retry semântico.
- Narrativa livre não pode conter algarismos; action codes vêm de catálogo fechado; a LLM nunca escolhe urgência, destinatários, assunto ou cadência.
- Retenção analítica de 90 dias; segredos fora do repositório; logs guardam hashes, métricas e request IDs, nunca prompts, respostas, e-mails ou authorization headers.
- Contrato, prompt e catálogo são instalados em diretório absoluto versionado; nenhum loader depende do current working directory do serviço Windows.
- Cada tarefa segue TDD e termina em commit próprio.

## Mapa de arquivos

```text
pyproject.toml
config/statistical-analyst.example.toml
resources/
  contracts/operational_v1.json  # criado pelo plano Core; somente consumido aqui
  prompts/statistical_analyst_v1.txt
  catalogs/action_codes_v1.json
scripts/
  install-statistical-analyst.ps1
  uninstall-statistical-analyst.ps1
  verify-statistical-analyst.ps1
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
    openai_contract.py
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
    openai_client.py
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
    windows_service.py
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

class OpenAITransport(Protocol):
    async def analyze(self, prepared: "PreparedOpenAIRequest", client_request_id: UUID) -> "ResponseEnvelope": ...

class ResponseValidator(Protocol):
    def validate(self, envelope: "ResponseEnvelope", sealed: "SealedInput") -> "ValidatedInterpretation | ValidationFailure": ...

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
    assert cfg.model == "gpt-5.6-luna"
    assert cfg.openai_credential_name == "AgenteMQTT/StatisticalAnalyst/OpenAI"

def test_config_rejects_recipient_inside_openai_payload_fields(tmp_path) -> None:
    path = write_config(tmp_path, payload_fields=["reservoir_id", "gmail_recipients"])
    with pytest.raises(ConfigError, match="allowlist"):
        load_analyst_config(path)

# tests/unit/test_analysis_models.py
def test_window_key_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        WindowKey(datetime(2026, 8, 12), "analysis-v1")

# tests/unit/test_analyst_secrets.py
def test_windows_provider_reads_current_value_on_every_request(fake_credential_backend) -> None:
    provider = WindowsCredentialProvider(fake_credential_backend)
    fake_credential_backend.set("AgenteMQTT/StatisticalAnalyst/OpenAI", "first")
    assert provider.get_required("AgenteMQTT/StatisticalAnalyst/OpenAI") == "first"
    fake_credential_backend.set("AgenteMQTT/StatisticalAnalyst/OpenAI", "rotated")
    assert provider.get_required("AgenteMQTT/StatisticalAnalyst/OpenAI") == "rotated"
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_analyst_config.py tests/unit/test_analyst_secrets.py tests/unit/test_analysis_models.py -v`

Expected: FAIL porque o pacote não existe.

- [ ] **Step 3: Adicionar pins e modelos mínimos**

```toml
# modificar as tabelas existentes em pyproject.toml; não redeclarar cabeçalhos TOML
[project.optional-dependencies]
analytics = ["openai==3.0.0", "pydantic==2.13.4"]
test = ["pytest==9.1.1", "pytest-cov==7.1.0", "pytest-asyncio==1.4.0"]

[tool.pytest.ini_options]
asyncio_mode = "strict"
markers = ["live: calls a paid external API", "operational: requires Windows service privileges", "mosquitto: requires local Mosquitto", "soak: long-running operational test"]
```

Ao editar, preservar `testpaths` e `addopts` criados pelo Core, adicionar `pytest-asyncio` à lista `test` existente e criar a chave `analytics`; um teste de parsing com `tomllib` deve assegurar que o arquivo final contém uma única tabela de cada nome.

Atualizar os lockfiles existentes com hashes de todas as dependências transitivas e reinstalar no mesmo ambiente isolado criado pelo plano Core:

```powershell
.venv\Scripts\python.exe -m pip install --require-hashes -r requirements/production.lock
.venv\Scripts\python.exe -m pip install --require-hashes -r requirements/test.lock
.venv\Scripts\python.exe -m pip install --no-build-isolation --no-deps -e .
.venv\Scripts\python.exe -m pip check
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

O TOML deve conter caminhos absolutos separados, `resource_directory=C:\\ProgramData\\AgenteMQTT\\StatisticalAnalyst\\resources`, timezone, modelo fixo, horários, snapshot delay, deadlines, limites, Gmail e nomes de targets do Windows Credential Manager (`openai_credential_name` e `gmail_credential_name`); nenhum segredo. Rejeitar chaves extras, modelo divergente, banco inexistente no preflight e diretórios graváveis compartilhados com o Core, exceto leitura do arquivo operacional. `WindowsCredentialProvider` chama `win32cred.CredRead` sob a identidade corrente em toda solicitação, decodifica somente `CRED_TYPE_GENERIC`, devolve erro seguro para target ausente e nunca faz cache/log do valor; isso permite rotação sem reiniciar e é reutilizado por OpenAI e Gmail.

- [ ] **Step 4: Executar testes e checar import das versões fixadas**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_analyst_config.py tests/unit/test_analyst_secrets.py tests/unit/test_analysis_models.py -v`

Expected: PASS.

Run: `.venv\Scripts\python.exe -c "import openai,pydantic; assert openai.__version__=='3.0.0'; assert pydantic.__version__=='2.13.4'"`

Expected: exit code 0 no ambiente instalado pelos locks.

- [ ] **Step 5: Commit**

```powershell
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

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_analytics_schema.py tests/integration/test_analysis_store.py tests/integration/test_analyst_wheel_artifact.py -v`

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

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_analytics_schema.py tests/integration/test_analysis_store.py tests/integration/test_analyst_wheel_artifact.py -v`

Expected: PASS, inclusive dois criadores concorrentes obtendo o mesmo batch e wheel isolado capaz de migrar sem source tree.

- [ ] **Step 5: Commit**

```powershell
git add pyproject.toml src/statistical_analyst/persistence tests/integration/test_analytics_schema.py tests/integration/test_analysis_store.py tests/integration/test_analyst_wheel_artifact.py
git commit -m "feat: persist isolated analytics execution state"
```

### Task 3: Leitor read-only e snapshot operacional consistente

**Files:**
- Consume installed copy: `<resource_directory>\contracts\operational_v1.json`
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

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_operational_snapshot.py tests/integration/test_operational_query_deadline.py -v`

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

Antes de ler dados, comparar versão, `ready=1`, `timestamp_format`, `rule_version`, SHA-256 e a matriz exata de colunas obtida por `PRAGMA table_info` com o caminho absoluto `<resource_directory>\contracts\operational_v1.json` da configuração. O teste muda o CWD para um diretório vazio e ainda captura o snapshot. O Analyst implementa seu próprio parser estrito para `YYYY-MM-DDTHH:MM:SS.ffffffZ`; não importa serializer, store nem migration do Core. Divergência vira fallback de snapshot e zero HTTP.

As consultas devem selecionar explicitamente as colunas do manifesto, nunca `SELECT *`; usar bounds formatados canonicamente e intervalos semiabertos por `received_at_utc`; ler `raw_events` com `LEFT JOIN measurements` por `event_id`, incluindo separadamente o bit bruto `duplicate` e `delivery_disposition`, além de `state_transitions`, o log append-only `alarms`, `runtime_health` e `operational_contract`; ordenar por timestamps e IDs. O DTO rejeita disposição fora de `NEW/FIRST_LOCAL_COPY/ALREADY_COMMITTED`. Para cada intervalo e cada par `(reservoir_id,machine)`, executar uma query para a última transição com `effective_at_utc < start` e uni-la às transições `effective_at_utc >= start AND effective_at_utc < end`; deduplicar por `transition_id`. Isso fornece o left carry separadamente para qualidade e processo nas janelas atual, 24h e sete dias. Alarmes ativos são derivados pela última lifecycle de cada `alarm_id`, sendo `RECOVERED` inativo e `STATUS_UPDATED` ainda ativo. O deadline total passado pelo caller é `start+10s`.

Converter `sqlite3.OperationalError` causado pelo progress handler em `SnapshotDeadlineExceeded`, fazer rollback e fechar a conexão. O snapshot guarda `contract_version`, `rule_version` e `manifest_sha256`; esses valores entram no hash selado.

- [ ] **Step 4: Executar testes com writer concorrente e query lenta**

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_operational_snapshot.py tests/integration/test_operational_query_deadline.py -v`

Expected: PASS; query interrompida antes de 10,5s gera `SnapshotDeadlineExceeded` e não tenta novamente nessa execução.

- [ ] **Step 5: Commit**

```powershell
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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_analyst_scheduler.py tests/integration/test_batch_recovery.py -v`

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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_analyst_scheduler.py tests/integration/test_batch_recovery.py -v`

Expected: PASS; dois schedulers concorrentes produzem um batch e um relatório lógico.

- [ ] **Step 5: Commit**

```powershell
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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_slot_selection.py tests/unit/test_sufficiency.py -v`

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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_slot_selection.py tests/unit/test_sufficiency.py -v`

Expected: PASS, inclusive gap exatamente no limite e um segundo acima.

- [ ] **Step 5: Commit**

```powershell
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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_descriptive_statistics.py -v`

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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_descriptive_statistics.py -v`

Expected: PASS para n=0,1,2,>=3, bordas e um segundo acima de 10 minutos.

- [ ] **Step 5: Commit**

```powershell
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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_analysis_trends.py -v`

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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_analysis_trends.py -v`

Expected: PASS, incluindo timestamps iguais, ETA zero/null/válida e horizonte.

- [ ] **Step 5: Commit**

```powershell
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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_historical_comparison.py -v`

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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_historical_comparison.py -v`

Expected: PASS para 3/4/5/6 dias, 3/4 completos, robust z abaixo/exato/acima 3.5 e razões 0.67/1.50 inclusivas.

- [ ] **Step 5: Commit**

```powershell
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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_state_durations.py tests/unit/test_local_classification.py -v`

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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_state_durations.py tests/unit/test_local_classification.py -v`

Expected: PASS; nenhuma API/model aparece nesses módulos.

- [ ] **Step 5: Commit**

```powershell
git add src/statistical_analyst/analysis/state_durations.py src/statistical_analyst/analysis/classification.py tests/unit/test_state_durations.py tests/unit/test_local_classification.py
git commit -m "feat: classify analysis status and local priority"
```

### Task 10: Package builder, allowlist e selagem antes de HTTP

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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_package_builder.py tests/unit/test_security_scan.py tests/integration/test_sealed_input_restart.py -v`

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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_package_builder.py tests/unit/test_security_scan.py tests/integration/test_sealed_input_restart.py -v`

Expected: PASS; restart devolve bytes idênticos e evento tardio aparece somente em manifesto futuro.

- [ ] **Step 5: Commit**

```powershell
git add src/statistical_analyst/analysis/package_builder.py src/statistical_analyst/security.py src/statistical_analyst/persistence/analysis_store.py tests/unit/test_package_builder.py tests/unit/test_security_scan.py tests/integration/test_sealed_input_restart.py
git commit -m "feat: seal allowlisted analysis packages"
```

### Task 11: Contrato Pydantic de saída, prompt e catálogo

**Files:**
- Modify: `pyproject.toml`
- Create: `src/statistical_analyst/domain/openai_contract.py`
- Create: `src/statistical_analyst/ai/prompt.py`
- Create: `resources/prompts/statistical_analyst_v1.txt`
- Create: `resources/catalogs/action_codes_v1.json`
- Create: `tests/unit/test_openai_schema.py`
- Create: `tests/unit/test_prompt_catalog.py`
- Create: `tests/fixtures/golden/openai_schema_v1.json`
- Modify: `tests/integration/test_analyst_wheel_artifact.py`

**Interfaces:**
- Consumes: evidence IDs e versão da Task 10.
- Produces: `AnalysisResponse`, `Finding`, `RecommendedCheck`, `canonical_openai_schema_v1()` gerado do Pydantic, `load_prompt_v1(resource_directory)` e catálogo fechado.

- [ ] **Step 1: Escrever testes do schema e catálogo exatos**

```python
# tests/unit/test_openai_schema.py
def test_generated_schema_matches_reviewed_fixture() -> None:
    schema = canonical_openai_schema_v1()
    assert schema == load_json("tests/fixtures/golden/openai_schema_v1.json")
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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_openai_schema.py tests/unit/test_prompt_catalog.py tests/integration/test_analyst_wheel_artifact.py -v`

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

`AnalysisResponse` é a fonte única do schema: `canonical_openai_schema_v1()` chama `model_json_schema()`, normaliza deterministicamente para o subconjunto Structured Outputs e calcula o hash versionado. `tests/fixtures/golden/openai_schema_v1.json` é somente uma fixture de regressão gerada/comparada no teste, não um arquivo de runtime nem uma segunda fonte. O prompt deve ordenar: usar apenas métricas; não recalcular; não relacionar reservatórios; aceitar status/tendência/ETA/prioridade locais; sem comandos; output apenas no schema; narrativa sem algarismos. Os loaders recebem o `resource_directory` absoluto, nunca consultam CWD, calculam SHA-256 e falham se versão/hash não corresponder ao catálogo de recursos esperado. Reinstalar o editable package e testar a presença de contrato, prompt e catálogo no data path do venv.

- [ ] **Step 4: Executar testes e validar schema contra subconjunto OpenAI**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_openai_schema.py tests/unit/test_prompt_catalog.py tests/integration/test_analyst_wheel_artifact.py -v`

Expected: PASS; todos os campos required, `additionalProperties=false` em cada objeto, enums e maxItems corretos.

- [ ] **Step 5: Commit**

```powershell
git add pyproject.toml src/statistical_analyst/domain/openai_contract.py src/statistical_analyst/ai/prompt.py resources tests/unit/test_openai_schema.py tests/unit/test_prompt_catalog.py tests/integration/test_analyst_wheel_artifact.py tests/fixtures/golden/openai_schema_v1.json
git commit -m "feat: define strict consultative OpenAI contract"
```

### Task 12: Validador do envelope bruto e fallback semântico

**Files:**
- Create: `src/statistical_analyst/ai/response_validator.py`
- Create: `tests/unit/test_response_validator.py`

**Interfaces:**
- Consumes: `AnalysisResponse`, `SealedInput`, `ResponseEnvelope` neutro ao SDK.
- Produces: `ValidatedInterpretation | ValidationFailure`; nenhuma repetição.

- [ ] **Step 1: Escrever matriz completa dos estados Responses API**

```python
# tests/unit/test_response_validator.py
@pytest.mark.parametrize("envelope", [
    envelope(status="incomplete", incomplete_reason="max_output_tokens"),
    envelope(status="failed"), envelope(status="completed", refusal="cannot comply"),
    envelope(status="completed", output=[]), envelope(status="completed", messages=2),
    envelope(status="completed", output_texts=2), envelope(status="completed", tool_item=True),
])
def test_noncanonical_response_is_fallback(envelope, sealed_input) -> None:
    result = validate_response(envelope, sealed_input)
    assert isinstance(result, ValidationFailure)
    assert result.retry_allowed is False

def test_reasoning_item_is_allowed_but_only_one_visible_message(sealed_input) -> None:
    result = validate_response(valid_envelope(reasoning_items=1), sealed_input)
    assert isinstance(result, ValidatedInterpretation)

@pytest.mark.parametrize("text", ["nível 1", "abrir válvula", "parar bomba", "setpoint", "override"])
def test_digits_and_control_language_are_rejected(text, sealed_input) -> None:
    result = validate_response(valid_envelope(executive_summary=text), sealed_input)
    assert isinstance(result, ValidationFailure)
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_response_validator.py -v`

Expected: FAIL por validador ausente.

- [ ] **Step 3: Implementar pipeline de validação em ordem fixa**

```python
def validate_response(envelope: ResponseEnvelope, sealed: SealedInput) -> ValidationResult:
    if envelope.status != "completed" or envelope.incomplete_details is not None:
        return failure("RESPONSE_NOT_COMPLETED")
    if envelope.refusal is not None or envelope.error is not None:
        return failure("REFUSAL_OR_ERROR")
    if len(envelope.visible_messages) != 1 or len(envelope.output_texts) != 1 or envelope.has_tool_output:
        return failure("OUTPUT_CARDINALITY")
    raw = envelope.output_texts[0].encode("utf-8")
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

Validar todas as refs no catálogo de entrada; unicidade; nenhum algarismo Unicode em texto livre; denylist de controle com normalização casefold; action codes no catálogo. Guardar raw para auditoria no banco analítico, mas nunca enviar se falhar. `ValidationFailure.retry_allowed` é sempre false para esta Task.

- [ ] **Step 4: Executar matriz inteira**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_response_validator.py -v`

Expected: PASS para completed válido, reasoning opaco permitido e todo edge state rejeitado.

- [ ] **Step 5: Commit**

```powershell
git add src/statistical_analyst/ai/response_validator.py tests/unit/test_response_validator.py
git commit -m "feat: validate OpenAI responses before operator use"
```

### Task 13: Orçamento móvel, circuit breaker e retry policy

**Files:**
- Create: `src/statistical_analyst/ai/retry_policy.py`
- Create: `src/statistical_analyst/ai/circuit_breaker.py`
- Modify: `src/statistical_analyst/persistence/usage_store.py`
- Create: `tests/unit/test_retry_policy.py`
- Create: `tests/unit/test_circuit_breaker.py`
- Create: `tests/integration/test_usage_concurrency.py`

**Interfaces:**
- Consumes: Clock e `analytics.sqlite`.
- Produces: `UsageBudget.try_reserve/reconcile`, `classify_error`, `next_retry`, `CircuitBreaker.allow/record`.

- [ ] **Step 1: Escrever testes dos limites e erro sem usage**

```python
# tests/integration/test_usage_concurrency.py
def test_twenty_fourth_reservation_succeeds_and_twenty_fifth_fails(two_usage_stores, now) -> None:
    reservations = concurrent_reserve(two_usage_stores, count=25, now=now)
    assert sum(item is not None for item in reservations) == 24

def test_timeout_keeps_full_reservation_for_twenty_four_hours(usage_store, now) -> None:
    reservation = usage_store.try_reserve(uuid4(), now)
    usage_store.reconcile(reservation.reservation_id, usage=None, now=now + minutes(1))
    assert usage_store.totals(now + hours(23)).input_tokens == 5000
    assert usage_store.totals(now + hours(24) + seconds(1)).input_tokens == 0

def test_reconciled_usage_replaces_reserved_amount_without_losing_attempt(usage_store, now) -> None:
    reservation = usage_store.try_reserve(uuid4(), now)
    usage_store.reconcile(reservation.reservation_id, usage=TokenUsage(input_tokens=1200, output_tokens=800), now=now)
    totals = usage_store.totals(now)
    assert (totals.attempts, totals.input_tokens, totals.output_tokens) == (1, 1200, 800)

# tests/unit/test_retry_policy.py
@pytest.mark.parametrize("status", [408, 409, 500, 502, 503])
def test_retryable_http_statuses(status) -> None:
    assert classify_error(http_error(status)).retryable is True

def test_temporal_429_is_retryable_only_for_positive_allowlist() -> None:
    assert classify_error(http_error(429, code="rate_limit_exceeded")).retryable is True
    assert classify_error(http_error(429, code=None)).retryable is False
    assert classify_error(http_error(429, code="unknown_usage_limit")).retryable is False

@pytest.mark.parametrize("code", [
    "credit_balance_exhausted", "organization_spend_limit_exceeded",
    "project_spend_limit_exceeded", "usage_limit_exceeded",
])
def test_spend_and_usage_errors_never_retry(code) -> None:
    assert classify_error(http_error(429, code=code)).retryable is False

@pytest.mark.parametrize("error", [network_error(), http_error(401), http_error(403), schema_error()])
def test_network_retries_but_auth_permission_and_configuration_do_not(error) -> None:
    expected = isinstance(error, NetworkError)
    assert classify_error(error).retryable is expected

def test_retry_after_delta_and_http_date_are_minimum_with_jitter(now) -> None:
    delta = next_retry(http_error(429, code="rate_limit_exceeded", retry_after="12"), now, jitter=2, deadline=now + seconds(30))
    dated = next_retry(http_error(503, retry_after=format_http_date(now + seconds(10))), now, jitter=2, deadline=now + seconds(30))
    assert delta == now + seconds(14)
    assert dated == now + seconds(12)

def test_retry_is_refused_when_wait_would_cross_global_deadline(now) -> None:
    assert next_retry(http_error(503, retry_after="60"), now, jitter=0, deadline=now + seconds(30)) is None

def test_physical_retry_reserves_a_distinct_attempt_and_success_resets_transient_count(retry_harness) -> None:
    retry_harness.fail_once(network_error()).then_succeed()
    result = retry_harness.run()
    assert result.status == "completed"
    assert retry_harness.distinct_reservation_count == 2
    assert retry_harness.breaker.consecutive_transient_failures == 0
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_retry_policy.py tests/unit/test_circuit_breaker.py tests/integration/test_usage_concurrency.py -v`

Expected: FAIL por policy/store incompletos.

- [ ] **Step 3: Implementar reserva `BEGIN IMMEDIATE` e policies puras**

```python
def try_reserve(self, attempt_id: UUID, now: datetime, input_tokens: int = 5000, output_tokens: int = 4000):
    cutoff = now - timedelta(hours=24)
    with self._transaction("IMMEDIATE") as conn:
        attempts, inputs, outputs = conn.execute(
            "SELECT count(*),coalesce(sum(accounted_input_tokens),0),coalesce(sum(accounted_output_tokens),0) FROM usage_reservations WHERE reserved_at_utc>?",
            (iso(cutoff),),
        ).fetchone()
        if attempts >= 24 or inputs + input_tokens > 120000 or outputs + output_tokens > 96000:
            return None
        return insert_reservation(conn, attempt_id, now, input_tokens, output_tokens)
```

Reconcile com usage confiável reduz reserva aos valores reais; sem usage mantém 5000/4000. Se o uso real exceder qualquer reserva, persistir o uso real, abrir o breaker de configuração e impedir novas chamadas até nova validação e restart controlado. Retry: máximo 1; rede/408/409/5xx e 429 somente quando o código pertence à allowlist temporal versionada. Qualquer 429 desconhecido ou limite de uso é não transitório por padrão seguro. Interpretar `Retry-After` tanto em delta-seconds quanto HTTP-date, usá-lo como mínimo, somar jitter injetável e recusar espera além do deadline; sem header válido, exponencial+jitter <=30s. Breaker abre 24h para auth/crédito/spend; transitório após três falhas consecutivas até próxima janela e volta a zero após sucesso; configuração pode fechar somente em restart controlado. Existe uma única camada de retry e cada tentativa física recebe `attempt_id`/reserva distintos antes de enviar bytes.

- [ ] **Step 4: Executar testes de corrida, rolling window e breaker persistente**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_retry_policy.py tests/unit/test_circuit_breaker.py tests/integration/test_usage_concurrency.py -v`

Expected: PASS; retry físico só ocorre após uma nova reserva bem-sucedida.

- [ ] **Step 5: Commit**

```powershell
git add src/statistical_analyst/ai/retry_policy.py src/statistical_analyst/ai/circuit_breaker.py src/statistical_analyst/persistence/usage_store.py tests/unit/test_retry_policy.py tests/unit/test_circuit_breaker.py tests/integration/test_usage_concurrency.py
git commit -m "feat: enforce OpenAI retry and usage budgets"
```

### Task 14: Adapter oficial OpenAI Responses API

**Files:**
- Create: `src/statistical_analyst/ai/request_builder.py`
- Create: `src/statistical_analyst/ai/openai_client.py`
- Create: `tests/unit/test_openai_request.py`
- Create: `tests/integration/test_openai_sdk_boundary.py`

**Interfaces:**
- Consumes: `SealedInput`, resource directory absoluto, `SecretProvider`, `AnalysisResponse`, UsageBudget e retry policy.
- Produces: `RequestBuilder(resource_directory).prepare(sealed) -> PreparedOpenAIRequest` com guard de bytes e `OfficialOpenAITransport.analyze(prepared,...) -> ResponseEnvelope` neutro ao SDK.

- [ ] **Step 1: Escrever teste do request exato e tradução do envelope**

```python
# tests/unit/test_openai_request.py
@pytest.mark.asyncio
async def test_request_has_fixed_model_and_no_tools_or_history(sealed_input, request_builder, recording_sdk) -> None:
    transport = OfficialOpenAITransport(lambda: recording_sdk)
    prepared = request_builder.prepare(sealed_input)
    await transport.analyze(prepared, UUID("12345678-1234-4234-8234-123456789abc"))
    request = recording_sdk.last_request
    assert request["model"] == "gpt-5.6-luna"
    assert request["reasoning"] == {"effort": "low"}
    assert request["store"] is False
    assert request["background"] is False
    assert request["truncation"] == "disabled"
    assert request["max_output_tokens"] == 4000
    assert request["text_format"] is AnalysisResponse
    assert "tools" not in request and "previous_response_id" not in request
    assert request["extra_headers"]["X-Client-Request-Id"] == "12345678-1234-4234-8234-123456789abc"
    assert prepared.full_text_size_bytes <= 16_000

def test_combined_prompt_input_and_schema_over_limit_fails_before_http(oversized_sealed_input, request_builder, recording_sdk) -> None:
    with pytest.raises(RequestPayloadTooLarge):
        request_builder.prepare(oversized_sealed_input)
    assert recording_sdk.call_count == 0

# tests/integration/test_openai_sdk_boundary.py
@pytest.mark.asyncio
async def test_serialized_http_body_contains_strict_reviewed_schema(sealed_input, request_builder, recording_http) -> None:
    transport = OfficialOpenAITransport.from_http_client(recording_http)
    await transport.analyze(request_builder.prepare(sealed_input), uuid4())
    body = recording_http.last_json_body
    assert body["text"]["format"]["type"] == "json_schema"
    assert body["text"]["format"]["strict"] is True
    assert body["text"]["format"]["schema"] == load_json("tests/fixtures/golden/openai_schema_v1.json")
    assert "tools" not in body and "previous_response_id" not in body

def test_sdk_envelope_preserves_raw_cardinality_and_ids(fake_sdk_response) -> None:
    envelope = translate_response(fake_sdk_response)
    assert envelope.response_id == fake_sdk_response.id
    assert envelope.request_id == fake_sdk_response._request_id
    assert len(envelope.raw_output_items) == len(fake_sdk_response.output)

@pytest.mark.asyncio
async def test_each_physical_attempt_rereads_rotated_windows_credential(prepared_request, rotating_secret_provider, recording_client_factory) -> None:
    factory = CredentialBackedOpenAIClientFactory(
        rotating_secret_provider, "AgenteMQTT/StatisticalAnalyst/OpenAI", recording_client_factory
    )
    transport = OfficialOpenAITransport(factory)
    rotating_secret_provider.value = "first"
    await transport.analyze(prepared_request, uuid4())
    rotating_secret_provider.value = "rotated"
    await transport.analyze(prepared_request, uuid4())
    assert recording_client_factory.api_keys == ["first", "rotated"]
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_openai_request.py tests/integration/test_openai_sdk_boundary.py -v`

Expected: FAIL por adapter ausente.

- [ ] **Step 3: Implementar SDK com retries automáticos desativados**

```python
class OfficialOpenAITransport:
    def __init__(self, client_factory: Callable[[], AsyncOpenAI]) -> None:
        self._client_factory = client_factory

    async def analyze(self, prepared: PreparedOpenAIRequest, client_request_id: UUID) -> ResponseEnvelope:
        client = self._client_factory()  # relê Credential Manager em cada tentativa física
        response = await client.responses.parse(
            model="gpt-5.6-luna",
            instructions=prepared.instructions,
            input=prepared.input_text,
            text_format=AnalysisResponse,
            reasoning={"effort": "low"},
            store=False,
            background=False,
            truncation="disabled",
            max_output_tokens=4000,
            extra_headers={"X-Client-Request-Id": str(client_request_id)},
        )
        return translate_response(response)
```

`CredentialBackedOpenAIClientFactory` recebe `SecretProvider`, target name e um construtor SDK injetável; chama `get_required()` em cada tentativa física e cria `AsyncOpenAI(api_key=..., max_retries=0, timeout=60.0)`. Não existe cache estático da chave. Um factory separado, aceitando `OPENAI_API_KEY`, é permitido somente pela fixture live opt-in e nunca pelo composition root do serviço. `RequestBuilder` carrega somente o prompt do `resource_directory` absoluto e obtém o schema em memória de `canonical_openai_schema_v1()`/`AnalysisResponse`; nunca lê a fixture de testes. Constrói a mesma representação textual que será serializada no body (`instructions`, JSON selado e JSON Schema, incluindo chaves/separadores estruturais), mede os bytes UTF-8 e reprova acima de 16.000. Esse guard roda antes de breaker, reserva e HTTP; não é apenas assert de fixture. O teste de boundary inspeciona o JSON HTTP depois da serialização do SDK para comprovar `text.format.type=json_schema`, `strict=true` e igualdade com a fixture dourada, além do teste unitário do argumento `text_format=AnalysisResponse`.

Não usar somente `response.output_text` ou `output_parsed`; traduzir status, incomplete, todos os output items/contents, refusal, usage, model, response id e `_request_id`. Classificar exceções do SDK sem persistir headers/body sensíveis. Se a versão 3.0.0 não aceitar algum parâmetro no teste de boundary, adaptar somente ao nome oficial dessa versão e manter o request sem tools/history.

- [ ] **Step 4: Executar boundary test sem rede**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_openai_request.py tests/integration/test_openai_sdk_boundary.py -v`

Expected: PASS usando transporte HTTP fake, comprovando guard real de 16.000 bytes, Structured Output estrito, `max_retries=0`, timeout 60s e envelope completo.

- [ ] **Step 5: Commit**

```powershell
git add src/statistical_analyst/ai/request_builder.py src/statistical_analyst/ai/openai_client.py tests/unit/test_openai_request.py tests/integration/test_openai_sdk_boundary.py
git commit -m "feat: call fixed OpenAI Responses contract"
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
    assert result.sections["0051"].source == "openai"
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
async def test_snapshot_deadline_returns_two_fallbacks_without_http(coordinator, reader, transport, budget) -> None:
    reader.error = SnapshotDeadlineExceeded()
    result = await coordinator.run_batch(WINDOW_KEY)
    assert {section.fallback_reason for section in result.sections.values()} == {"OPERATIONAL_SNAPSHOT_UNAVAILABLE"}
    assert transport.calls == []
    assert budget.reservation_count == 0

@pytest.mark.asyncio
async def test_oversized_prepared_request_falls_back_before_reservation(coordinator, request_builder, transport, budget) -> None:
    request_builder.error_for["0051"] = RequestPayloadTooLarge(16_001)
    result = await coordinator.run_batch(WINDOW_KEY)
    assert result.sections["0051"].fallback_reason == "OPENAI_REQUEST_TOO_LARGE"
    assert "0051" not in transport.calls
    assert budget.reservations_for("0051") == 0
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_analysis_coordinator.py tests/integration/test_batch_end_to_end.py -v`

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
            early_fallbacks[rid] = self._fallback(sealed[rid], "OPENAI_REQUEST_TOO_LARGE")
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

`_snapshot_failure_result` persiste o motivo seguro no batch e produz as duas seções locais sem pacote OpenAI nem HTTP; o renderizador ainda cria um único relatório de indisponibilidade operacional. Somente exceções esperadas de leitura/contrato viram esse fallback; `CancelledError` e falhas fatais de persistência preservam semântica de shutdown/fatal do serviço.

Cada tentativa: request já validado, breaker allow, reserva, chamada, persistir response/usage, reconcile, validar. Retry transitório usa nova reserva. Output model diferente do solicitado é registrado e tratado como falha de contrato. Toda exceção de API vira `SectionResult` local; nunca escapa para o MQTT Core. Ao cancelar por deadline, reconciliar sem usage (mantendo 5000/4000), aguardar todas as tasks com `gather(return_exceptions=True)` e somente então retornar; nenhuma coroutine pode gravar depois do `BatchResult`.

- [ ] **Step 4: Executar testes de restart em cada checkpoint**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_analysis_coordinator.py tests/integration/test_batch_end_to_end.py -v`

Expected: PASS para falha de snapshot, limite de request, falha antes/depois de selar, antes/depois de resposta e antes de report; restart reutiliza pacote, cancelamentos estão concluídos e budgets nunca são liberados sem usage confiável.

- [ ] **Step 5: Commit**

```powershell
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
    first = build_fallback(package, reason="OPENAI_TIMEOUT", resource_directory=resource_directory)
    second = build_fallback(package, reason="OPENAI_TIMEOUT", resource_directory=resource_directory)
    assert first == second
    assert set(first.action_codes) <= load_action_catalog(resource_directory).keys()
```

- [ ] **Step 2: Executar os testes e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_local_fallback.py tests/unit/test_report_renderer.py -v`

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

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_local_fallback.py tests/unit/test_report_renderer.py -v`

Expected: PASS; snapshots de texto e HTML são estáveis e nenhuma saída da LLM altera assunto, destinatários, prioridade, cadência ou números locais.

- [ ] **Step 5: Commit**

```powershell
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

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_report_outbox.py tests/integration/test_analytics_gmail.py -v`

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

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_report_outbox.py tests/integration/test_analytics_gmail.py -v`

Expected: PASS; falha antes do envio é repetida, report já confirmado não é reenfileirado e toda repetição mantém fingerprint e `Message-ID`.

- [ ] **Step 5: Commit**

```powershell
git add src/statistical_analyst/persistence/analysis_store.py src/statistical_analyst/persistence/outbox_store.py src/statistical_analyst/reporting/gmail_transport.py src/statistical_analyst/reporting/dispatcher.py tests/integration/test_report_outbox.py tests/integration/test_analytics_gmail.py
git commit -m "feat: deliver reports through isolated Gmail outbox"
```

### Task 18: Runtime, serviço Windows, observabilidade e proteção local

**Files:**
- Consume unchanged: `resources/contracts/operational_v1.json`
- Consume unchanged: `resources/prompts/statistical_analyst_v1.txt`
- Consume unchanged: `resources/catalogs/action_codes_v1.json`
- Create: `src/statistical_analyst/__main__.py`
- Create: `src/statistical_analyst/observability.py`
- Modify: `src/statistical_analyst/security.py`
- Create: `src/statistical_analyst/service/runtime.py`
- Create: `src/statistical_analyst/service/windows_service.py`
- Create: `scripts/install-statistical-analyst.ps1`
- Create: `scripts/uninstall-statistical-analyst.ps1`
- Create: `scripts/verify-statistical-analyst.ps1`
- Create: `tests/unit/test_analyst_runtime.py`
- Create: `tests/unit/test_analyst_log_redaction.py`
- Create: `tests/integration/test_analytics_retention_backup.py`
- Create: `tests/integration/test_operational_readiness.py`
- Create: `tests/operational/test_statistical_analyst_service.py`

**Interfaces:**
- Consumes: config validada, coordinator, scheduler, dispatcher, stop event e caminhos absolutos em `%ProgramData%`.
- Produces: `AnalystRuntime.start/stop`, serviço `StatisticalAnalyst`, logs estruturados redigidos, retenção/backup de 90 dias e preflight real de implantação.

- [ ] **Step 1: Escrever testes de lifecycle, redaction e retenção**

```python
# tests/unit/test_analyst_runtime.py
def test_stop_cancels_analysis_and_drains_completed_outbox(runtime, fake_components) -> None:
    runtime.start()
    runtime.stop(grace_seconds=30)
    assert fake_components.scheduler.stopped
    assert fake_components.openai.cancelled
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
    redacting_logger.analysis_failed(api_key="sk-secret", recipient="x@gmail.com", response='{"summary":"raw"}')
    assert "sk-secret" not in caplog.text
    assert "x@gmail.com" not in caplog.text
    assert '"summary":"raw"' not in caplog.text

# tests/integration/test_analytics_retention_backup.py
def test_cleanup_keeps_last_ninety_days_and_backup_is_consistent(analytics_db, now, backup_path) -> None:
    seed_analytics(analytics_db, ages_days=[89, 90, 91])
    result = cleanup_and_backup(analytics_db, backup_path, now)
    assert result.deleted_older_than_cutoff == 1
    assert integrity_check(backup_path) == "ok"
```

- [ ] **Step 2: Executar os testes e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_analyst_runtime.py tests/unit/test_analyst_log_redaction.py tests/integration/test_analytics_retention_backup.py tests/integration/test_operational_readiness.py -v`

Expected: FAIL por runtime e rotinas ausentes.

- [ ] **Step 3: Implementar composição, serviço e preflight seguro**

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
        self._dispatcher.start(self._stop_event)
        self._scheduler.start(self._stop_event)

    def stop(self, grace_seconds: float = 30.0) -> None:
        self._stop_event.set()
        self._coordinator.cancel_pending()
        self._scheduler.join(timeout=grace_seconds)
        self._dispatcher.drain_confirmed(timeout=grace_seconds)
        self._analytics_db.close()
```

`OperationalReadiness` abre somente `mode=ro`, espera com backoff limitado até 120 segundos por arquivo, linha `operational_contract.ready=1`, `runtime_health.core_ready=1`, versão/hash/rule version conhecidos e schema exato; respeita stop event e jamais cria/migra tabela operacional. Versão/hash incompatível falha imediatamente; ausência ou `ready=0` pode esperar até o timeout e então encerra o serviço com código não zero para recovery do SCM.

`windows_service.py` usa `pywin32`, traduz SCM stop para o mesmo event e configura o processo como `BELOW_NORMAL_PRIORITY_CLASS`; nunca importa nem inicia o runtime do MQTT Core. O instalador exige conta de serviço dedicada, caminhos absolutos distintos, automatic delayed start e recovery actions e registra `AgenteMqttCore` como dependência SCM explícita de `StatisticalAnalyst`; testa em reboot que o Core chega a RUNNING/ready antes da análise. Aplica ACL somente à conta e administradores. Antes de alterar ACL/serviço, valida exatamente cada destino e aborta se estiver fora do diretório configurado. Para `operational.sqlite` e seu diretório, exige ACE explícita Read+Traverse da conta analítica e verifica ausência de Create/Write/Delete tanto no arquivo principal quanto em `-wal`/`-shm`; o preflight tenta criar um arquivo sentinela e executar UPDATE sob o token real da conta e ambos devem falhar.

O instalador resolve as fontes empacotadas em `<venv-data>\share\agente-mqtt\contracts` e `<venv-data>\share\agente-mqtt\statistical-analyst`, verifica os hashes aprovados e copia contrato, prompt e catálogo via temporário + rename para os subdiretórios de `resource_directory`; aplica ACL somente de leitura à conta analítica. O composition root injeta esse diretório em reader, request builder e renderer. Serviço e verificadores falham antes do scheduler se qualquer recurso faltar/divergir e o teste executa com CWD fora do repositório.

O composition root de produção instancia um único `WindowsCredentialProvider` sob a conta do serviço e o injeta nos factories OpenAI/Gmail. O instalador não recebe nem grava valores secretos: imprime os dois target names, exige que sejam provisionados no Credential Manager da conta dedicada e executa um probe redigido sob esse token. O teste operacional provisiona um target descartável, lê-o através do processo do serviço, rotaciona o valor, executa nova tentativa fake (sem rede) e comprova que o novo valor foi usado; em seguida revoga o target e confirma falha segura sem revelar o conteúdo. `OPENAI_API_KEY` é aceito somente pelo harness live interativo, nunca pelo Windows Service.

`verify-statistical-analyst.ps1` deve reprovar implantação se o banco operacional não puder ser aberto read-only, se `analytics.sqlite` for gravável por identidades não autorizadas, ou se volumes de banco/backup não informarem BitLocker ativo. Backup usa `sqlite3.Connection.backup()`, não cópia direta de `.sqlite`/WAL. O teste unitário não simula conformidade de ACL/BitLocker; o teste operacional roda em Windows elevado e verifica o estado real.

- [ ] **Step 4: Executar testes portáveis e smoke test Windows**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_analyst_runtime.py tests/unit/test_analyst_log_redaction.py tests/integration/test_analytics_retention_backup.py tests/integration/test_operational_readiness.py -v`

Expected: PASS.

Run em runner Windows administrativo: `.venv\Scripts\python.exe -m pytest tests/operational/test_statistical_analyst_service.py -v --run-operational`

Expected: serviço instala com dependência `AgenteMqttCore`, inicia com prioridade BELOW_NORMAL somente após contrato ready, carrega recursos absolutos e credenciais rotacionáveis sob sua conta, para em até 30 segundos, reinicia por SCM/reboot e mantém os dois bancos/outboxes isolados.

- [ ] **Step 5: Commit**

```powershell
git add src/statistical_analyst/__main__.py src/statistical_analyst/observability.py src/statistical_analyst/security.py src/statistical_analyst/service scripts/install-statistical-analyst.ps1 scripts/uninstall-statistical-analyst.ps1 scripts/verify-statistical-analyst.ps1 tests/unit/test_analyst_runtime.py tests/unit/test_analyst_log_redaction.py tests/integration/test_analytics_retention_backup.py tests/integration/test_operational_readiness.py tests/operational/test_statistical_analyst_service.py
git commit -m "feat: operate isolated statistical analyst service"
```

### Task 19: Gates dourados, isolamento E2E, testes live e piloto

**Files:**
- Create: `tests/fixtures/golden/normal.json`
- Create: `tests/fixtures/golden/drying.json`
- Create: `tests/fixtures/golden/overflowing.json`
- Create: `tests/fixtures/golden/data_quality_failures.json`
- Create: `tests/fixtures/golden/historical_and_alarm_cases.json`
- Create: `tests/fixtures/golden/openai_failures.json`
- Create: `tests/e2e/test_mqtt_core_isolation.py`
- Create: `tests/e2e/test_retention_snapshot_contract.py`
- Create: `tests/e2e/test_full_analysis_report.py`
- Create: `tests/e2e/conftest.py`
- Create: `tests/e2e/harness.py`
- Create: `tests/live/test_openai_contract.py`
- Create: `tests/live/test_fixture_token_counts.py`
- Create: `src/statistical_analyst/pilot/__init__.py`
- Create: `src/statistical_analyst/pilot/evaluator.py`
- Create: `tests/unit/test_pilot_evaluator.py`
- Modify: `tests/conftest.py`
- Create: `scripts/run-statistical-pilot.ps1`
- Create: `scripts/evaluate-statistical-pilot.ps1`
- Create: `docs/runbooks/statistical-analyst-pilot.md`
- Create: `docs/runbooks/statistical-analyst-operations.md`

**Interfaces:**
- Consumes: sistema completo, broker/SMTP/OpenAI fakes, chave de projeto opt-in e ficha de avaliação humana.
- Produces: gates automatizados, relatório de piloto e decisão explícita `PROMOTE`, `EXTEND` ou `BLOCK`.

`tests/conftest.py` deve preservar a implementação única criada pelo Core; apenas completar o gating de `--run-live` e `--run-operational` se ainda necessário, sem redefinir opções. `tests/e2e/harness.py` possui `SystemHarness`, `GoldenHarness`, `GOLDEN_SCENARIOS` e builders dos fakes broker/SMTP/OpenAI; `tests/e2e/conftest.py` expõe essas fixtures. No teste de isolamento, cada runtime é iniciado em subprocesso separado; o processo analítico não importa o composition root do Core.

- [ ] **Step 1: Escrever matriz dourada e teste de isolamento crítico**

```python
# tests/e2e/test_mqtt_core_isolation.py
def test_slow_saturated_analyst_cannot_delay_core_persistence_or_critical_alert(system_harness) -> None:
    system_harness.openai.block_for(seconds=240)
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

Os fixtures cobrem: normal, secagem, extravasamento, variabilidade, ausência, travamento, inválido, salto `>0.4`, histórico insuficiente, alarme ativo, tentativa de reduzir alarme, timeout/408/409/5xx/429, crédito/spend/auth/permissão, schema inválido, budgets esgotados, ausência de usage e bloqueio de segredo/e-mail.

- [ ] **Step 2: Executar suites sem rede e confirmar falhas iniciais**

Run: `.venv\Scripts\python.exe -m pytest tests/unit tests/integration tests/e2e -m "not live and not operational and not mosquitto and not soak" -v`

Expected: FAIL até fixtures, harness e verificações finais existirem.

- [ ] **Step 3: Implementar testes live opt-in e avaliação do piloto**

```python
# tests/live/test_fixture_token_counts.py
@pytest.mark.live
@pytest.mark.parametrize("fixture", all_request_fixtures())
def test_official_input_token_count_is_within_budget(openai_client, fixture) -> None:
    count = openai_client.responses.input_tokens.count(**build_count_request(fixture))
    assert count.input_tokens <= 5000

# tests/live/test_openai_contract.py
@pytest.mark.live
def test_luna_returns_strict_completed_contract(live_transport, request_builder, sealed_normal_input) -> None:
    prepared = request_builder.prepare(sealed_normal_input)
    envelope = run(live_transport.analyze(prepared, uuid4()))
    assert envelope.status == "completed"
    assert envelope.model == "gpt-5.6-luna"
    assert validate_response(envelope, sealed_normal_input).is_valid
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

Testes `live` exigem simultaneamente `--run-live`, `OPENAI_API_KEY` do projeto dedicado e `OPENAI_LIVE_COST_ACCEPTED=YES`; ficam fora do CI normal. Registrar separadamente custo, request IDs e contagens, sem guardar conteúdo em logs. `run-statistical-pilot.ps1` coleta continuamente desde a ativação, sem parar em 42 execuções quando a decisão for `EXTEND`, e encerra somente em `PROMOTE`, `BLOCK` ou 14 dias. `evaluate-statistical-pilot.ps1` chama o mesmo `PilotEvaluator` Python, não reimplementa fórmulas em PowerShell, e emite relatório assinado com os 14 gates e denominadores.

O runbook de operações cobre instalação do wheel sem checkout, paths/hashes dos três recursos, targets do Credential Manager, rotação e revogação sob a conta de serviço, dependência/ready do Core, recuperação de `DELIVERY_UNCERTAIN`, circuit breaker/budget, restore do banco analítico e rollback que preserva a versão anterior de recursos. Todos os comandos de verificação devem funcionar com CWD fora do repositório.

- [ ] **Step 4: Executar todos os gates apropriados**

Run: `.venv\Scripts\python.exe -m pytest tests/unit tests/integration tests/e2e -m "not live and not operational and not mosquitto and not soak" -v --cov=agente_mqtt --cov=statistical_analyst --cov-report=term-missing --cov-fail-under=90`

Expected: PASS; nenhuma chamada de rede real.

Run autorizado antes do piloto: `.venv\Scripts\python.exe -m pytest tests/live -v --run-live`

Expected: PASS para acesso ao modelo, Structured Output e todas as fixtures com `input_tokens <= 5000`.

Run no host Windows de piloto: `powershell -ExecutionPolicy Bypass -File scripts/verify-statistical-analyst.ps1`

Expected: PASS para conta, ACL, BitLocker, paths, serviço, acesso read-only e bancos separados.

- [ ] **Step 5: Commit**

```powershell
git add src/statistical_analyst/pilot tests/unit/test_pilot_evaluator.py tests/conftest.py tests/fixtures/golden tests/e2e tests/live scripts/run-statistical-pilot.ps1 scripts/evaluate-statistical-pilot.ps1 docs/runbooks/statistical-analyst-pilot.md docs/runbooks/statistical-analyst-operations.md
git commit -m "test: gate statistical analyst pilot"
```

## Verificação final deste plano

- [ ] Executar `.venv\Scripts\python.exe -m pytest tests/unit tests/integration tests/e2e -m "not live and not operational and not mosquitto and not soak" -v --cov=agente_mqtt --cov=statistical_analyst --cov-report=term-missing --cov-fail-under=90`.
- [ ] Executar `.venv\Scripts\python.exe -m compileall -q src tests`.
- [ ] Executar `.venv\Scripts\python.exe -m pip check` no ambiente criado pelos lockfiles.
- [ ] Executar `rg -n "TODO|FIXME|pass$|NotImplemented|example\.invalid|sk-" src config scripts docs/runbooks` e resolver qualquer ocorrência executável ou segredo.
- [ ] Verificar que `operational.sqlite` abre com `mode=ro`, `query_only=ON` e que `UPDATE` falha no teste de integração.
- [ ] Inspecionar o request capturado: modelo fixo, reasoning low, sem tools/history/background, `store=false`, truncation disabled, timeout e retry únicos.
- [ ] Autorizar custo e executar `tests/live` somente com a chave dedicada antes do piloto.
- [ ] Executar `scripts/verify-statistical-analyst.ps1` no host Windows real e anexar sua saída ao registro de implantação.
- [ ] Rodar o piloto de 7 dias, ou estender até 14 dias conforme a regra de amostra, e obter decisão técnica explícita antes de ativar produção.
