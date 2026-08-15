# MQTT Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir o serviço Windows determinístico que recebe continuamente os dois tópicos MQTT, persiste eventos auditáveis, detecta riscos de nível e falhas de medição e entrega alertas pelo Gmail sem controlar equipamentos.

**Architecture:** Um único processo `agente_mqtt` usa Paho MQTT com QoS 1 e ACK manual, um escritor SQLite serializado e máquinas de estado puras. Ingestão, transições, alarmes e outbox são gravados na mesma transação; o ACK só ocorre após o commit. Agendamento, SMTP e serviço Windows ficam atrás de interfaces injetáveis para que as regras sejam testadas sem rede ou relógio real.

**Tech Stack:** CPython 3.12 x64, biblioteca padrão (`sqlite3`, `ssl`, `smtplib`, `tomllib`, `zoneinfo`, `logging`), `paho-mqtt==2.1.0`, `pywin32==312`, `tzdata==2026.3`, `pytest==9.1.1`, `pytest-cov==7.1.0`.

## Global Constraints

- Implementar primeiro este plano completo; o Statistical Analyst OpenAI é outro processo e outro plano.
- Monitorar somente `EPZ/META/0051/ANI01/RAW` e `EPZ/META/0022/ANI01/RAW`; a credencial MQTT não pode publicar.
- Converter por `pressure_bar = raw_value / 1024`, preservando o valor numérico sem arredondar antes das comparações.
- Limites: baixo crítico `<=1.0 bar`, atenção baixa `<=1.05 bar`, atenção alta `>=1.15 bar`, alto crítico `>=1.2 bar`.
- Mensagem esperada a cada 5 minutos; avaliação consolidada nos minutos `00`, `15`, `30` e `45` de `America/Fortaleza`.
- Ausência: warning após 15 minutos e critical após 30 minutos; travamento: seis mensagens iguais cobrindo pelo menos 30 minutos.
- Salto estritamente maior que `0.4 bar` em até 15 minutos entra em quarentena.
- O serviço é apenas supervisório: nenhuma chamada MQTT publish e nenhuma interface de bomba, válvula ou PLC.
- TLS com validação de certificado é obrigatório fora de fixture local; mínimo TLS 1.2.
- Usar timestamps UTC aware no domínio e `ZoneInfo("America/Fortaleza")` somente para alinhamento operacional.
- SQLite em WAL, foreign keys habilitadas, transações explícitas e conexões não compartilhadas entre threads.
- Segredos somente por Windows Credential Manager ou variável protegida da conta de serviço; nunca em TOML, log ou Git.
- SMTP não garante exactly-once após resultado incerto. A garantia implementável é: uma notificação lógica por fingerprint e `Message-ID` determinístico; um aceite SMTP seguido de falha antes do `mark_sent` pode ser reenviado.
- Cada tarefa segue red-green-refactor e termina com commit próprio; não combinar tarefas adjacentes.

## Mapa de arquivos

```text
pyproject.toml
requirements/
  build.lock
  production.lock
  test.lock
config/
  mqtt-core.example.toml
resources/contracts/
  operational_v1.json
scripts/
  install-mqtt-core.ps1
  uninstall-mqtt-core.ps1
  configure-mqtt-core-recovery.ps1
  verify-mqtt-core.ps1
src/agente_mqtt/
  __init__.py
  __main__.py
  application.py
  clock.py
  config.py
  secrets.py
  domain/
    models.py
    decoding.py
    trend.py
    quality_machine.py
    process_machine.py
    alarms.py
    fingerprints.py
  mqtt/
    envelope.py
    backoff.py
    client.py
  storage/
    database.py
    writer.py
    repositories.py
    backup.py
    retention.py
    migrations/0001_operational.sql
  scheduling/
    scheduler.py
    maintenance.py
  notifications/
    templates.py
    outbox.py
    gmail_smtp.py
  observability/
    json_logging.py
    health.py
  service/
    paths.py
    windows_service.py
tests/
  conftest.py
  unit/
  integration/
  operational/
  fixtures/mosquitto.conf
```

## Interfaces estáveis entre tarefas

```python
from collections.abc import Callable
from datetime import datetime
from typing import Protocol, TypeVar

T = TypeVar("T")
U = TypeVar("U")

class Clock(Protocol):
    def utc_now(self) -> datetime: ...
    def monotonic(self) -> float: ...

class SecretProvider(Protocol):
    def get_required(self, target_name: str) -> str: ...

class DatabaseWriter(Protocol):
    def execute(self, operation: Callable[["UnitOfWork"], T]) -> T: ...
    def execute_staged(self, prepare: Callable[["UnitOfWork"], T],
                       apply: Callable[["UnitOfWork", T], "U"]) -> "U": ...

class MessageHandler(Protocol):
    def handle(self, envelope: "MqttEnvelope") -> "PersistedEvent": ...

class SessionEpochStore(Protocol):
    def observe_connect(self, session_present: bool) -> int: ...
    def current(self) -> int: ...

class RuleEngine(Protocol):
    def ingest(self, event: "PersistedEvent", uow: "UnitOfWork") -> None: ...
    def evaluate(self, reservoir_id: str, window_end_utc: datetime, uow: "UnitOfWork") -> None: ...

class MailTransport(Protocol):
    def send(self, message: "OutgoingEmail") -> "SendReceipt": ...

class CoreApplication(Protocol):
    def start(self) -> None: ...
    def stop(self, deadline_seconds: float) -> None: ...
```

---

### Task 1: Scaffold, dependências e configuração estrita

**Files:**
- Create: `pyproject.toml`
- Create: `requirements/build.lock`
- Create: `requirements/production.lock`
- Create: `requirements/test.lock`
- Create: `config/mqtt-core.example.toml`
- Create: `src/agente_mqtt/__init__.py`
- Create: `src/agente_mqtt/clock.py`
- Create: `src/agente_mqtt/config.py`
- Create: `src/agente_mqtt/secrets.py`
- Create: `src/agente_mqtt/domain/__init__.py`
- Create: `src/agente_mqtt/mqtt/__init__.py`
- Create: `src/agente_mqtt/storage/__init__.py`
- Create: `src/agente_mqtt/scheduling/__init__.py`
- Create: `src/agente_mqtt/notifications/__init__.py`
- Create: `src/agente_mqtt/observability/__init__.py`
- Create: `src/agente_mqtt/service/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/unit/test_config.py`

**Interfaces:**
- Consumes: nenhuma; esta é a raiz do projeto.
- Produces: `CoreConfig`, `load_config(path: Path) -> CoreConfig`, `SystemClock`, `WindowsCredentialProvider` e as constantes `RESERVOIR_TOPICS`.

- [ ] **Step 1: Escrever os testes de configuração e o bootstrap mínimo**

```python
# tests/unit/test_config.py
from pathlib import Path
import pytest

from agente_mqtt.config import RESERVOIR_TOPICS, ConfigError, load_config

def test_load_config_accepts_exact_topics_and_absolute_paths(tmp_path: Path) -> None:
    cfg_path = tmp_path / "core.toml"
    cfg_path.write_text(valid_core_toml(tmp_path), encoding="utf-8")
    cfg = load_config(cfg_path)
    assert cfg.database_path.is_absolute()
    assert cfg.resource_directory.is_absolute()
    assert tuple(cfg.topics) == tuple(RESERVOIR_TOPICS.values())

def test_load_config_rejects_relative_database_path(tmp_path: Path) -> None:
    cfg_path = tmp_path / "bad.toml"
    cfg_path.write_text(valid_core_toml(tmp_path).replace((tmp_path / "operational.sqlite").as_posix(), "relative.sqlite"), encoding="utf-8")
    with pytest.raises(ConfigError, match="absolute"):
        load_config(cfg_path)

def test_rules_and_retention_are_loaded_and_validated(tmp_path: Path) -> None:
    cfg = load_config(write_core_config(tmp_path, valid_core_toml(tmp_path)))
    assert (cfg.raw_units_per_bar, cfg.critical_low_bar, cfg.critical_high_bar) == (1024.0, 1.0, 1.2)
    assert (cfg.raw_retention_days, cfg.transition_retention_days, cfg.backup_keep_files) == (90, 365, 30)
```

`valid_core_toml()` escreve todas as tabelas obrigatórias `[topics]`, `[mqtt]`, `[gmail]`, `[rules]` e `[retention]`, com os valores aprovados listados no Step 3; testes negativos alteram somente o campo sob teste.

Criar neste passo o `pyproject.toml` mínimo com `src` layout e os três lockfiles completos; ainda não criar `agente_mqtt.config`. Isso permite que o primeiro vermelho prove módulo ausente em um ambiente limpo, em vez de falhar por `pytest` não instalado.

- [ ] **Step 2: Executar o teste e confirmar a falha esperada**

Run:

```powershell
py -3.12 -m venv .venv
.venv\Scripts\python.exe -m pip install --require-hashes -r requirements/build.lock
.venv\Scripts\python.exe -m pip install --require-hashes -r requirements/production.lock
.venv\Scripts\python.exe -m pip install --require-hashes -r requirements/test.lock
.venv\Scripts\python.exe -m pytest tests/unit/test_config.py -v
```

Expected: FAIL durante import porque `agente_mqtt.config` ainda não existe.

- [ ] **Step 3: Criar o pacote, pins e implementação mínima da configuração**

```toml
# pyproject.toml
[build-system]
requires = ["setuptools==80.9.0"]
build-backend = "setuptools.build_meta"

[project]
name = "agente-mqtt-operacao"
version = "0.1.0"
requires-python = ">=3.12,<3.13"
dependencies = [
  "paho-mqtt==2.1.0",
  "pywin32==312; sys_platform == 'win32'",
  "tzdata==2026.3",
]

[project.optional-dependencies]
test = ["pytest==9.1.1", "pytest-cov==7.1.0"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]
addopts = "-ra --strict-markers"
markers = [
  "operational: requires Windows service privileges",
  "live: calls a paid external API",
  "mosquitto: requires a local Mosquitto executable",
  "soak: long-running operational test",
]
```

```python
# src/agente_mqtt/config.py
from dataclasses import dataclass
from datetime import time as LocalTime
from pathlib import Path
from zoneinfo import ZoneInfo
import tomllib

RESERVOIR_TOPICS = {
    "0051": "EPZ/META/0051/ANI01/RAW",
    "0022": "EPZ/META/0022/ANI01/RAW",
}

class ConfigError(ValueError):
    pass

@dataclass(frozen=True)
class CoreConfig:
    timezone: ZoneInfo
    database_path: Path
    log_directory: Path
    backup_directory: Path
    resource_directory: Path
    topics: tuple[str, str]
    mqtt_host: str
    mqtt_port: int
    mqtt_protocol: str
    mqtt_client_id: str
    mqtt_ca_file: Path
    allow_insecure_local_test: bool
    gmail_sender: str
    gmail_recipients: tuple[str, ...]
    raw_units_per_bar: float
    critical_low_bar: float
    low_warning_bar: float
    high_warning_bar: float
    critical_high_bar: float
    expected_interval_minutes: int
    evaluation_minutes: int
    communication_lost_minutes: int
    missing_warning_minutes: int
    missing_critical_minutes: int
    stuck_messages: int
    stuck_minutes: int
    jump_limit_bar: float
    jump_lookback_minutes: int
    trend_min_samples: int
    trend_min_span_minutes: int
    trend_horizon_minutes: int
    confirmation_min_minutes: int
    dry_recovery_bar: float
    low_warning_recovery_bar: float
    overflow_recovery_bar: float
    high_warning_recovery_bar: float
    reminder_minutes: int
    smtp_retry_max_minutes: int
    integrity_local_time: LocalTime
    maintenance_local_time: LocalTime
    raw_retention_days: int
    transition_retention_days: int
    backup_keep_files: int
    rule_version: str

def _absolute(value: str, field: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise ConfigError(f"{field} must be absolute")
    return path

def load_config(path: Path) -> CoreConfig:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    required = {"timezone", "database_path", "log_directory", "backup_directory", "resource_directory", "topics", "mqtt", "gmail", "rules", "retention"}
    if set(raw) != required:
        raise ConfigError(f"unexpected or missing root keys: {sorted(set(raw) ^ required)}")
    mqtt = raw["mqtt"]
    gmail = raw["gmail"]
    return CoreConfig(
        timezone=ZoneInfo(raw["timezone"]),
        database_path=_absolute(raw["database_path"], "database_path"),
        log_directory=_absolute(raw["log_directory"], "log_directory"),
        backup_directory=_absolute(raw["backup_directory"], "backup_directory"),
        resource_directory=_absolute(raw["resource_directory"], "resource_directory"),
        topics=validate_exact_topics(raw["topics"], RESERVOIR_TOPICS),
        mqtt_host=str(mqtt["host"]), mqtt_port=int(mqtt["port"]),
        mqtt_protocol=str(mqtt["protocol"]), mqtt_client_id=str(mqtt["client_id"]),
        mqtt_ca_file=_absolute(mqtt["ca_file"], "mqtt.ca_file"),
        allow_insecure_local_test=bool(mqtt["allow_insecure_local_test"]),
        gmail_sender=str(gmail["sender"]),
        gmail_recipients=tuple(str(v) for v in gmail["recipients"]),
        **validate_fixed_rules(raw["rules"]),
        **validate_retention(raw["retention"]),
    )
```

`resource_directory` é absoluto e aponta para `C:\\ProgramData\\AgenteMQTT\\Core\\resources`; nenhum loader pode depender do current working directory do SCM. `[topics]` deve conter exatamente o mapeamento `0051/0022` aprovado, sem curingas nem terceiro tópico. `[rules]` traz e valida escala `1024`, limites `1.0/1.05/1.15/1.2`, publicação esperada `5`, avaliação `15`, comunicação `5`, missing `15/30`, stuck `6/30`, salto `0.4` com lookback `15`, tendência `3` amostras/`10` minutos/horizonte `15`, confirmação mínima `4`, reminder `60`, retry SMTP máximo `60`, integridade `08:00`, manutenção diária padrão `02:00` e toda histerese aprovada. `[retention]` traz raw/measurements `90`, transitions/alarms `365` e backups `30`; estes prazos permanecem configuráveis com limites seguros, enquanto alterações de escala/limites/regras exigem nova `rule_version` e aprovação. Adicionar teste que muda retenção dentro do intervalo aceito e rejeita tópicos diferentes, limites invertidos, escala não positiva, tempos não crescentes ou chaves extras.

Implementar `SystemClock.utc_now()` com `datetime.now(timezone.utc)` e, em `clock.py`, importar o módulo como `import time as time_module` para implementar `SystemClock.monotonic()` com `time_module.monotonic()` sem colidir com `LocalTime`. `WindowsCredentialProvider.get_required()` usa `win32cred.CredRead` somente em Windows. O TOML de exemplo deve conter os campos acima, valores não secretos e caminhos `C:\\ProgramData\\AgenteMQTT\\...`. `tests/conftest.py` registra desde esta tarefa `--run-operational`, `--run-live`, `--run-mosquitto`, `--run-soak`, `--mqtt-host` e `--duration-hours`; markers externos são pulados por padrão e só rodam com seu opt-in correspondente.

Os três lockfiles devem conter versões e hashes completos, inclusive dependências transitivas e `setuptools==80.9.0`. Após criar o pacote, concluir o bootstrap reproduzível:

```powershell
.venv\Scripts\python.exe -m pip install --no-build-isolation --no-deps -e .
.venv\Scripts\python.exe -m pip check
```

Configurar também `[tool.setuptools.packages.find] where = ["src"]`. Todos os comandos Python das tarefas seguintes usam explicitamente `.venv\Scripts\python.exe`; nunca dependem de pacotes globais.

- [ ] **Step 4: Executar testes e inspeção de segredos**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_config.py -v`

Expected: PASS.

Run: `git grep -n -E "(password|api[_-]?key|BEGIN PRIVATE KEY)" -- ':!docs/' ':!config/*.example.toml'`

Expected: nenhuma credencial literal.

- [ ] **Step 5: Commit**

```powershell
git add pyproject.toml requirements config src/agente_mqtt/__init__.py src/agente_mqtt/clock.py src/agente_mqtt/config.py src/agente_mqtt/secrets.py src/agente_mqtt/domain/__init__.py src/agente_mqtt/mqtt/__init__.py src/agente_mqtt/storage/__init__.py src/agente_mqtt/scheduling/__init__.py src/agente_mqtt/notifications/__init__.py src/agente_mqtt/observability/__init__.py src/agente_mqtt/service/__init__.py tests/conftest.py tests/unit/test_config.py
git commit -m "build: scaffold MQTT core configuration"
```

### Task 2: Modelos do domínio, decodificação e fingerprints

**Files:**
- Create: `src/agente_mqtt/domain/models.py`
- Create: `src/agente_mqtt/domain/decoding.py`
- Create: `src/agente_mqtt/domain/fingerprints.py`
- Create: `src/agente_mqtt/mqtt/envelope.py`
- Create: `tests/unit/test_decoding.py`
- Create: `tests/unit/test_fingerprints.py`

**Interfaces:**
- Consumes: `RESERVOIR_TOPICS` da Task 1.
- Produces: `MqttEnvelope`, `DecodedMeasurement`, `DecodeFailure`, `decode_payload(envelope)`, `reservoir_for_topic(topic)` e `fingerprint(parts) -> str`.

- [ ] **Step 1: Escrever testes de limites, payload e fingerprint**

```python
# tests/unit/test_decoding.py
from datetime import datetime, timezone
import pytest
from agente_mqtt.domain.decoding import DecodeErrorCode, decode_payload
from agente_mqtt.mqtt.envelope import MqttEnvelope

def envelope(payload: bytes) -> MqttEnvelope:
    return MqttEnvelope(
        topic="EPZ/META/0051/ANI01/RAW", payload=payload,
        received_at=datetime(2026, 8, 12, tzinfo=timezone.utc),
        qos=1, retain=False, duplicate=False, packet_id=7,
    )

@pytest.mark.parametrize(("payload", "bar"), [(b"272", 0.265625), (b"1024", 1.0), (b"1229", 1.2001953125), (b"4096", 4.0)])
def test_decode_valid_payload(payload: bytes, bar: float) -> None:
    result = decode_payload(envelope(payload))
    assert result.raw_value == float(payload)
    assert result.pressure_bar == bar

@pytest.mark.parametrize("payload", [b"", b"abc", b"NaN", b"inf", b"-1", b"4096.1", b"1 2"])
def test_decode_rejects_invalid_payload(payload: bytes) -> None:
    result = decode_payload(envelope(payload))
    assert result.error_code in set(DecodeErrorCode)

def test_retained_message_is_decoded_but_not_confirmed_activity() -> None:
    env = envelope(b"1024")
    retained = MqttEnvelope(**{**env.__dict__, "retain": True})
    assert decode_payload(retained).eligible_for_activity is False
```

```python
# tests/unit/test_fingerprints.py
from agente_mqtt.domain.fingerprints import fingerprint

def test_fingerprint_is_stable_and_order_sensitive() -> None:
    assert fingerprint("alarm", "0051", "DRY_RISK", "2026-08-12T00:00:00Z") == fingerprint(
        "alarm", "0051", "DRY_RISK", "2026-08-12T00:00:00Z"
    )
    assert fingerprint("0051", "alarm") != fingerprint("alarm", "0051")
```

- [ ] **Step 2: Executar os testes para observar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_decoding.py tests/unit/test_fingerprints.py -v`

Expected: FAIL por módulos ausentes.

- [ ] **Step 3: Implementar modelos imutáveis e parser escalar completo**

```python
# src/agente_mqtt/domain/decoding.py
from dataclasses import dataclass
from enum import StrEnum
import math, re
from agente_mqtt.mqtt.envelope import MqttEnvelope

SCALAR = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")

class DecodeErrorCode(StrEnum):
    EMPTY = "EMPTY"
    UTF8 = "UTF8"
    FORMAT = "FORMAT"
    NON_FINITE = "NON_FINITE"
    OUT_OF_RANGE = "OUT_OF_RANGE"

@dataclass(frozen=True)
class DecodedMeasurement:
    raw_value: float
    pressure_bar: float
    eligible_for_activity: bool

@dataclass(frozen=True)
class DecodeFailure:
    error_code: DecodeErrorCode
    detail: str

def decode_payload(envelope: MqttEnvelope) -> DecodedMeasurement | DecodeFailure:
    try:
        text = envelope.payload.decode("utf-8").strip()
    except UnicodeDecodeError:
        return DecodeFailure(DecodeErrorCode.UTF8, "payload is not UTF-8")
    if not text:
        return DecodeFailure(DecodeErrorCode.EMPTY, "payload is empty")
    if not SCALAR.fullmatch(text):
        return DecodeFailure(DecodeErrorCode.FORMAT, "payload is not one scalar")
    value = float(text)
    if not math.isfinite(value):
        return DecodeFailure(DecodeErrorCode.NON_FINITE, "payload is non-finite")
    if not 0.0 <= value <= 4096.0:
        return DecodeFailure(DecodeErrorCode.OUT_OF_RANGE, "payload outside 0..4096")
    return DecodedMeasurement(value, value / 1024.0, not envelope.retain)
```

`MqttEnvelope` também carrega `client_id` e o `session_epoch` persistente fornecido pelo receiver. `MqttEnvelope.__post_init__` deve rejeitar timestamp naive, QoS diferente de 0/1/2, epoch/packet id negativos e tópico fora da allowlist. A flag `DUP` não torna a leitura inelegível por si só: ela pode ser a primeira cópia vista localmente após uma transação abortada. A decisão audit-only ocorre atomicamente no ledger da Task 4. `fingerprint(*parts)` deve serializar cada componente com prefixo de comprimento e retornar SHA-256 hexadecimal; não concatenar com separador ambíguo.

- [ ] **Step 4: Executar os testes do domínio**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_decoding.py tests/unit/test_fingerprints.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/domain src/agente_mqtt/mqtt tests/unit/test_decoding.py tests/unit/test_fingerprints.py
git commit -m "feat: decode and identify MQTT measurements"
```

### Task 3: Contrato SQL operacional e transações

**Files:**
- Modify: `pyproject.toml`
- Create: `resources/contracts/operational_v1.json`
- Create: `src/agente_mqtt/storage/migrations/0001_operational.sql`
- Create: `src/agente_mqtt/storage/database.py`
- Create: `src/agente_mqtt/storage/timestamps.py`
- Create: `src/agente_mqtt/storage/writer.py`
- Create: `src/agente_mqtt/storage/repositories.py`
- Create: `tests/unit/test_storage_timestamps.py`
- Create: `tests/integration/test_operational_schema.py`
- Create: `tests/integration/test_database_writer.py`
- Create: `tests/integration/test_core_wheel_artifact.py`
- Modify: `tests/conftest.py`

**Interfaces:**
- Consumes: modelos e fingerprints da Task 2.
- Produces: contrato SQL definitivo e versionado de `operational.sqlite`, manifesto `operational-v1`, `format_utc/parse_utc`, `open_connection(path, readonly=False)`, `migrate(path)`, `UnitOfWork` e `SerializedDatabaseWriter.execute()`.

- [ ] **Step 1: Escrever testes do schema e atomicidade**

```python
# tests/integration/test_operational_schema.py
import json
import sqlite3
from pathlib import Path
import pytest
from agente_mqtt.storage.database import migrate, open_connection
from agente_mqtt.storage.writer import SerializedDatabaseWriter

EXPECTED = {
    "schema_migrations", "raw_events", "measurements", "quality_state",
    "process_state", "state_transitions", "alarms", "outbox", "evaluations",
    "runtime_health", "operational_contract", "maintenance_jobs", "mqtt_deliveries",
}

def test_migration_is_reentrant_and_freezes_reader_contract(tmp_path, monkeypatch) -> None:
    path = tmp_path / "operational.sqlite"
    migrate(path); migrate(path)
    contract_path = Path(__file__).resolve().parents[2] / "resources" / "contracts" / "operational_v1.json"
    monkeypatch.chdir(tmp_path)  # o teste e o serviço não dependem do CWD do repositório
    with open_connection(path) as conn:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert EXPECTED <= tables
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
        contract = json.loads(contract_path.read_text("utf-8"))
        for table, expected_columns in contract["reader_columns"].items():
            actual = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
            assert actual == expected_columns
        health = conn.execute("SELECT * FROM runtime_health WHERE singleton_id=1").fetchone()
        assert (health["schema_version"], health["core_ready"]) == (1, 0)

def test_readonly_connection_is_query_only_and_sees_wal_commit(tmp_path) -> None:
    path = tmp_path / "operational.sqlite"; migrate(path)
    with open_connection(path) as writer, open_connection(path, readonly=True) as reader:
        writer.execute(
            "INSERT INTO raw_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("e1", "0051", "EPZ/META/0051/ANI01/RAW", b"1024",
             "2026-08-12T00:00:00.000000Z", "2026-08-12T00:00:00.000001Z",
             1, 0, 0, "NEW", 7, "VALID", None),
        )
        assert reader.execute("PRAGMA query_only").fetchone()[0] == 1
        assert reader.execute("SELECT count(*) FROM raw_events").fetchone()[0] == 1
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            reader.execute("UPDATE runtime_health SET mqtt_connected=1")

def test_trigger_foreign_keys_become_null_when_raw_audit_expires(tmp_path) -> None:
    path = tmp_path / "operational.sqlite"; migrate(path)
    with open_connection(path) as conn:
        for table in ("state_transitions", "alarms"):
            foreign_keys = conn.execute(f"PRAGMA foreign_key_list({table})").fetchall()
            assert any(row[3] == "trigger_event_id" and row[6] == "SET NULL" for row in foreign_keys)

def test_canonical_timestamps_make_half_open_boundaries_exact(tmp_path) -> None:
    path = tmp_path / "operational.sqlite"; migrate(path)
    timestamps = (
        ("start", "2026-08-12T00:00:00.000000Z"),
        ("inside", "2026-08-12T03:59:59.999999Z"),
        ("end", "2026-08-12T04:00:00.000000Z"),
    )
    with open_connection(path) as conn:
        conn.executemany(
            "INSERT INTO raw_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(event_id, "0051", "EPZ/META/0051/ANI01/RAW", b"1024", at, at,
              1, 0, 0, "NEW", index, "VALID", None)
             for index, (event_id, at) in enumerate(timestamps, start=1)],
        )
        ids = [r[0] for r in conn.execute(
            "SELECT event_id FROM raw_events WHERE received_at_utc>=? AND received_at_utc<? "
            "ORDER BY received_at_utc",
            ("2026-08-12T00:00:00.000000Z", "2026-08-12T04:00:00.000000Z"),
        )]
    assert ids == ["start", "inside"]

def test_measurement_and_outbox_rollback_with_event(tmp_path, raw_event, measurement, outgoing_email) -> None:
    path = tmp_path / "operational.sqlite"; migrate(path)
    writer = SerializedDatabaseWriter(path); writer.start()
    def failing_operation(uow) -> None:
        uow.raw_events.insert(raw_event("e1"))
        uow.measurements.insert(measurement("m1", event_id="e1"))
        uow.outbox.insert(outgoing_email("mail-e1"))
        raise RuntimeError("boom")
    with pytest.raises(RuntimeError, match="boom"):
        writer.execute(failing_operation)
    writer.stop(deadline_seconds=5)
    with open_connection(path, readonly=True) as conn:
        assert conn.execute("SELECT count(*) FROM raw_events").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM measurements").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0

def test_staged_writer_commits_intent_but_never_interleaves_evaluation(writer_harness) -> None:
    staged = writer_harness.start_staged(
        prepare=lambda uow: uow.trace.insert("intent"),
        apply=lambda uow, _: uow.trace.insert("effects"),
        pause_after_prepare_commit=True,
    )
    writer_harness.wait_for_prepare_commit()
    evaluation = writer_harness.enqueue(lambda uow: uow.trace.insert("evaluation"))
    writer_harness.release_apply()
    assert staged.result() == "effects"
    assert evaluation.result() == "evaluation"
    assert writer_harness.trace() == ["intent", "effects", "evaluation"]

def test_staged_writer_keeps_prepare_when_apply_rolls_back(writer_harness) -> None:
    with pytest.raises(InjectedWriteFailure):
        writer_harness.execute_staged(
            prepare=lambda uow: uow.trace.insert("intent"),
            apply=lambda uow, _: (_ for _ in ()).throw(InjectedWriteFailure()),
        )
    assert writer_harness.trace() == ["intent"]

# tests/integration/test_core_wheel_artifact.py
def test_isolated_wheel_contains_migration_and_contract(isolated_wheel_install) -> None:
    probe = isolated_wheel_install.run_resource_probe(cwd=isolated_wheel_install.empty_directory)
    assert probe.package_file("agente_mqtt.storage", "migrations/0001_operational.sql").exists
    assert probe.data_file("share/agente-mqtt/contracts/operational_v1.json").exists
    assert probe.migrate_fresh_database().schema_version == 1
```

```python
# tests/unit/test_storage_timestamps.py
from datetime import datetime, timezone
import pytest
from agente_mqtt.storage.timestamps import format_utc, parse_utc

def test_timestamp_is_fixed_width_rfc3339_utc_and_round_trips() -> None:
    value = datetime(2026, 8, 12, 3, 4, 5, 123, tzinfo=timezone.utc)
    encoded = format_utc(value)
    assert encoded == "2026-08-12T03:04:05.000123Z"
    assert parse_utc(encoded) == value

@pytest.mark.parametrize("bad", ["2026-08-12T03:04:05Z", "2026-08-12T03:04:05.000123+00:00"])
def test_timestamp_rejects_noncanonical_encodings(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_utc(bad)
```

- [ ] **Step 2: Executar os testes e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_storage_timestamps.py tests/integration/test_operational_schema.py tests/integration/test_database_writer.py tests/integration/test_core_wheel_artifact.py -v`

Expected: FAIL porque migração e writer não existem.

- [ ] **Step 3: Implementar schema completo e writer serializado**

Depois que o manifesto existir, acrescentar ao `pyproject.toml` existente, sem redeclarar cabeçalhos:

```toml
[tool.setuptools.data-files]
"share/agente-mqtt/contracts" = ["resources/contracts/operational_v1.json"]

[tool.setuptools.package-data]
"agente_mqtt.storage" = ["migrations/*.sql"]
```

Assim o wheel/venv contém a fonte versionada e a migration usada pelo instalador, sem depender de um checkout do repositório. `migrate()` abre o SQL com `importlib.resources.files("agente_mqtt.storage").joinpath("migrations/0001_operational.sql")`, nunca por CWD. A fixture `isolated_wheel_install` executa `pip wheel --no-deps --no-build-isolation`, instala o wheel em venv temporário sem editable/source tree e roda o probe com CWD vazio; teste de editable sozinho não é gate de empacotamento.

O SQL deve definir estes contratos consumidos depois pelo Statistical Analyst:

```sql
CREATE TABLE raw_events (
  event_id TEXT PRIMARY KEY,
  reservoir_id TEXT NOT NULL CHECK (reservoir_id IN ('0051','0022')),
  topic TEXT NOT NULL,
  payload BLOB NOT NULL,
  received_at_utc TEXT NOT NULL,
  persisted_at_utc TEXT NOT NULL,
  qos INTEGER NOT NULL CHECK (qos BETWEEN 0 AND 2),
  retain INTEGER NOT NULL CHECK (retain IN (0,1)),
  duplicate INTEGER NOT NULL CHECK (duplicate IN (0,1)),
  delivery_disposition TEXT NOT NULL CHECK (delivery_disposition IN ('NEW','FIRST_LOCAL_COPY','ALREADY_COMMITTED')),
  packet_id INTEGER,
  decode_status TEXT NOT NULL CHECK (decode_status IN ('VALID','INVALID','QUARANTINED')),
  error_code TEXT
);

CREATE TABLE measurements (
  measurement_id TEXT PRIMARY KEY,
  event_id TEXT NOT NULL UNIQUE REFERENCES raw_events(event_id),
  reservoir_id TEXT NOT NULL CHECK (reservoir_id IN ('0051','0022')),
  raw_value REAL NOT NULL CHECK (raw_value BETWEEN 0 AND 4096),
  pressure_bar REAL NOT NULL CHECK (pressure_bar BETWEEN 0 AND 4),
  received_at_utc TEXT NOT NULL,
  quality_state_at_insert TEXT NOT NULL,
  trusted_for_process INTEGER NOT NULL CHECK (trusted_for_process IN (0,1))
);

CREATE TABLE mqtt_deliveries (
  mqtt_client_id TEXT NOT NULL,
  session_epoch INTEGER NOT NULL CHECK (session_epoch >= 0),
  packet_id INTEGER NOT NULL CHECK (packet_id >= 0),
  topic TEXT NOT NULL,
  delivery_generation INTEGER NOT NULL CHECK (delivery_generation >= 1),
  payload_sha256 TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('RECEIVED','COMMITTED')),
  committed_event_id TEXT,
  received_at_utc TEXT NOT NULL,
  committed_at_utc TEXT,
  last_seen_at_utc TEXT NOT NULL,
  redelivery_count INTEGER NOT NULL DEFAULT 0 CHECK (redelivery_count >= 0),
  PRIMARY KEY (mqtt_client_id, session_epoch, packet_id, topic)
);

CREATE TABLE state_transitions (
  transition_id TEXT PRIMARY KEY,
  reservoir_id TEXT NOT NULL,
  machine TEXT NOT NULL CHECK (machine IN ('quality','process')),
  previous_state TEXT,
  new_state TEXT NOT NULL,
  effective_at_utc TEXT NOT NULL,
  recorded_at_utc TEXT NOT NULL,
  rule_version TEXT NOT NULL,
  trigger_kind TEXT NOT NULL,
  trigger_event_id TEXT REFERENCES raw_events(event_id) ON DELETE SET NULL,
  evaluation_id TEXT,
  fingerprint TEXT NOT NULL UNIQUE
);

CREATE TABLE alarms (
  alarm_event_id TEXT PRIMARY KEY,
  alarm_id TEXT NOT NULL,
  reservoir_id TEXT NOT NULL CHECK (reservoir_id IN ('0051','0022')),
  kind TEXT NOT NULL,
  severity TEXT NOT NULL CHECK (severity IN ('WARNING','CRITICAL')),
  lifecycle TEXT NOT NULL CHECK (lifecycle IN ('OPEN','ESCALATED','REMINDER','STATUS_UPDATED','RECOVERED')),
  effective_at_utc TEXT NOT NULL,
  recorded_at_utc TEXT NOT NULL,
  trigger_event_id TEXT REFERENCES raw_events(event_id) ON DELETE SET NULL,
  evaluation_id TEXT,
  reason_code TEXT NOT NULL,
  measurement_confirmed INTEGER CHECK (measurement_confirmed IN (0,1)),
  quality_state TEXT NOT NULL,
  confirmation_reason_code TEXT,
  fingerprint TEXT NOT NULL UNIQUE
);

CREATE TABLE quality_state (
  reservoir_id TEXT PRIMARY KEY CHECK (reservoir_id IN ('0051','0022')),
  state TEXT NOT NULL CHECK (state IN ('INITIALIZING','GOOD','STUCK','INVALID','MISSING','COMMUNICATION_LOST')),
  effective_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  activated_at_utc TEXT NOT NULL,
  last_new_message_at_utc TEXT,
  last_valid_measurement_at_utc TEXT,
  disconnected_since_utc TEXT,
  invalid_since_utc TEXT,
  invalid_count INTEGER NOT NULL DEFAULT 0 CHECK (invalid_count >= 0),
  equal_raw_value REAL,
  equal_count INTEGER NOT NULL DEFAULT 0 CHECK (equal_count >= 0),
  equal_first_at_utc TEXT,
  sixth_equal_at_utc TEXT,
  recovery_first_at_utc TEXT,
  version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0)
);

CREATE TABLE process_state (
  reservoir_id TEXT PRIMARY KEY CHECK (reservoir_id IN ('0051','0022')),
  state TEXT NOT NULL CHECK (state IN ('UNKNOWN','NORMAL','LOW_WARNING','DRY_RISK','HIGH_WARNING','OVERFLOW_RISK')),
  effective_at_utc TEXT NOT NULL,
  updated_at_utc TEXT NOT NULL,
  last_measurement_id TEXT REFERENCES measurements(measurement_id) ON DELETE SET NULL,
  last_pressure_bar REAL,
  confirmation_target TEXT,
  confirmation_count INTEGER NOT NULL DEFAULT 0 CHECK (confirmation_count >= 0),
  confirmation_first_at_utc TEXT,
  version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0)
);

CREATE TABLE evaluations (
  evaluation_id TEXT PRIMARY KEY,
  reservoir_id TEXT NOT NULL CHECK (reservoir_id IN ('0051','0022')),
  window_end_utc TEXT NOT NULL,
  rule_version TEXT NOT NULL,
  started_at_utc TEXT NOT NULL,
  completed_at_utc TEXT,
  status TEXT NOT NULL CHECK (status IN ('STARTED','COMPLETED','FAILED')),
  safe_error_code TEXT,
  UNIQUE (reservoir_id, window_end_utc, rule_version)
);

CREATE TABLE outbox (
  outbox_id TEXT PRIMARY KEY,
  alarm_event_id TEXT REFERENCES alarms(alarm_event_id) ON DELETE SET NULL,
  alarm_id TEXT,
  reservoir_id TEXT CHECK (reservoir_id IN ('0051','0022')),
  lifecycle TEXT NOT NULL CHECK (lifecycle IN ('OPEN','ESCALATED','REMINDER','RECOVERY','INTEGRITY')),
  priority TEXT NOT NULL CHECK (priority IN ('INFO','WARNING','CRITICAL')),
  message_id TEXT NOT NULL,
  payload_json BLOB NOT NULL,
  fingerprint TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL CHECK (status IN ('PENDING','SENDING','SENT','DEAD','DELIVERY_UNCERTAIN')),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  next_attempt_at_utc TEXT NOT NULL,
  lease_owner TEXT,
  lease_until_utc TEXT,
  provider_id TEXT,
  sent_at_utc TEXT,
  safe_error_code TEXT,
  created_at_utc TEXT NOT NULL
);

CREATE TABLE runtime_health (
  singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
  schema_version INTEGER NOT NULL CHECK (schema_version = 1),
  contract_version TEXT NOT NULL,
  rule_version TEXT NOT NULL,
  core_ready INTEGER NOT NULL CHECK (core_ready IN (0,1)),
  mqtt_session_epoch INTEGER NOT NULL DEFAULT 0 CHECK (mqtt_session_epoch >= 0),
  mqtt_connected INTEGER NOT NULL CHECK (mqtt_connected IN (0,1)),
  mqtt_subscription_ready INTEGER NOT NULL DEFAULT 0 CHECK (mqtt_subscription_ready IN (0,1)),
  disconnected_since_utc TEXT,
  reconnection_count INTEGER NOT NULL DEFAULT 0 CHECK (reconnection_count >= 0),
  last_message_at_utc TEXT,
  last_valid_at_utc TEXT,
  last_evaluation_at_utc TEXT,
  last_email_at_utc TEXT,
  updated_at_utc TEXT NOT NULL
);

CREATE TABLE operational_contract (
  singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
  schema_version INTEGER NOT NULL CHECK (schema_version = 1),
  contract_version TEXT NOT NULL CHECK (contract_version = 'operational-v1'),
  rule_version TEXT NOT NULL,
  timestamp_format TEXT NOT NULL CHECK (timestamp_format = 'RFC3339_UTC_MICROSECONDS_Z'),
  manifest_sha256 TEXT NOT NULL,
  activated_at_utc TEXT NOT NULL,
  ready INTEGER NOT NULL CHECK (ready IN (0,1))
);

CREATE TABLE maintenance_jobs (
  maintenance_job_id TEXT PRIMARY KEY,
  local_date TEXT NOT NULL,
  job_version TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('STARTED','COMPLETED','FAILED')),
  started_at_utc TEXT NOT NULL,
  completed_at_utc TEXT,
  backup_path TEXT,
  safe_error_code TEXT,
  UNIQUE (local_date, job_version)
);
```

O mesmo arquivo SQL inclui os índices por `(reservoir_id, received_at_utc)`, transições por `(reservoir_id,machine,effective_at_utc)`, alarmes por `(reservoir_id,effective_at_utc)` e outbox por `(status,next_attempt_at_utc)`, além de inserir a única linha de `runtime_health` com `schema_version=1`, `contract_version='operational-v1'`, `rule_version` ainda vazio, `core_ready=0`, `mqtt_session_epoch=0`, `mqtt_connected=0`, `mqtt_subscription_ready=0` e timestamps canônicos. `alarms` é append-only: a condição ativa é a última linha por `alarm_id` cuja lifecycle não foi seguida por `RECOVERED`; `STATUS_UPDATED` registra mudança de confiança sem fechar nem enviar novo alarme.

`resources/contracts/operational_v1.json` é a fonte de build compartilhada e canônica: lista, em ordem, todas as colunas das tabelas lidas pelo Statistical Analyst (`raw_events`, `measurements`, `state_transitions`, `alarms`, `runtime_health` e `operational_contract`), tipos/enums, os dois tópicos, reservatórios, limites, histereses, `rule_version` e `timestamp_format`. Na instalação, ela é copiada de forma atômica para `<resource_directory>\\contracts\\operational_v1.json`; em runtime, o Core usa somente esse caminho absoluto configurado e verifica o SHA-256, inclusive quando o CWD é `C:\\Windows\\System32`. A inicialização grava manifesto e regras com ambos os flags `ready` ainda em zero. Somente depois do catch-up completo a Task 14 marca `operational_contract.ready=1` e `runtime_health.core_ready=1` na mesma transação. Qualquer mudança sem nova versão/fixture falha antes de MQTT. O Analyst compara versão e hash de sua cópia instalada e nunca importa código runtime do Core.

Todo timestamp SQL usa exatamente `YYYY-MM-DDTHH:MM:SS.ffffffZ` em UTC (27 caracteres); `format_utc` rejeita datetime naive e `parse_utc` rejeita `+00:00`, ausência de micros ou offsets. Todas as binds de limites passam por esse serializer, de modo que comparação lexicográfica preserva ordem. O teste compartilhado cobre frações e bordas semiabertas: valor exatamente em `start` entra, exatamente em `end` não entra.

A migração aplica a cada coluna UTC obrigatória `CHECK (length(coluna)=27 AND substr(coluna,11,1)='T' AND substr(coluna,20,1)='.' AND substr(coluna,27,1)='Z')`; para colunas opcionais, o mesmo predicado é protegido por `coluna IS NULL OR (...)`. Isso torna o formato uma propriedade do banco, não só do adapter Python.

```python
# src/agente_mqtt/storage/database.py
def open_connection(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    uri = f"file:{path.as_posix()}?mode=ro" if readonly else str(path)
    conn = sqlite3.connect(uri, uri=readonly, timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    if readonly:
        conn.execute("PRAGMA query_only=ON")
    else:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
    return conn
```

`SerializedDatabaseWriter` deve possuir uma thread, uma `queue.Queue`, uma conexão própria e envelopes `Future`; cada operação executa `BEGIN IMMEDIATE`, recebe `UnitOfWork`, faz commit ou rollback e devolve resultado/exceção. `execute_staged(prepare, apply)` ocupa **um único item da fila**: faz BEGIN/commit de `prepare`, imediatamente faz novo BEGIN para `apply` e só depois retira outro item. Se `apply` falhar, seu rollback não desfaz a intenção preparada, mas nenhuma avaliação/manutenção consegue intercalar entre as fases. `stop()` não abandona operação já iniciada.

- [ ] **Step 4: Executar testes de schema, concorrência e rollback**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_storage_timestamps.py tests/integration/test_operational_schema.py tests/integration/test_database_writer.py tests/integration/test_core_wheel_artifact.py -v`

Expected: PASS, incluindo duas threads solicitando gravações, apenas uma conexão escritora e wheel isolado capaz de migrar sem checkout. Reinstalar também o ambiente de desenvolvimento com `.venv\Scripts\python.exe -m pip install --no-build-isolation --no-deps -e .`.

- [ ] **Step 5: Commit**

```powershell
git add pyproject.toml resources/contracts/operational_v1.json src/agente_mqtt/storage tests/conftest.py tests/unit/test_storage_timestamps.py tests/integration/test_operational_schema.py tests/integration/test_database_writer.py tests/integration/test_core_wheel_artifact.py
git commit -m "feat: define operational SQLite contract"
```

### Task 4: Persistência de eventos e quarentena por salto

**Files:**
- Modify: `src/agente_mqtt/storage/repositories.py`
- Create: `src/agente_mqtt/application.py`
- Create: `tests/integration/test_event_ingestion.py`

**Interfaces:**
- Consumes: `decode_payload`, `SerializedDatabaseWriter`, `UnitOfWork`.
- Produces: `PersistedEvent`, `EventIngestor.handle(envelope) -> PersistedEvent`; ainda sem MQTT real.

- [ ] **Step 1: Escrever testes de persistência, retained, duplicata e salto**

```python
# tests/integration/test_event_ingestion.py
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import pytest
from agente_mqtt.application import EventIngestor
from agente_mqtt.storage.writer import InjectedWriteFailure

T0 = datetime(2026, 8, 12, tzinfo=timezone.utc)

def test_jump_exactly_point_four_is_accepted(core_db, make_envelope, fake_clock, noop_rule_engine) -> None:
    ingestor = EventIngestor(core_db.writer, core_db.repositories, fake_clock, noop_rule_engine)
    t0 = datetime(2026, 8, 12, tzinfo=timezone.utc)
    first = ingestor.handle(make_envelope(raw=b"1024", at=t0))
    boundary = ingestor.handle(make_envelope(raw=b"1433.6", at=t0 + timedelta(minutes=5)))
    assert first.decode_status == "VALID"
    assert boundary.decode_status == "VALID"
    assert core_db.scalar("SELECT count(*) FROM measurements") == 2

def test_jump_over_point_four_is_quarantined(empty_core_db, make_envelope, fake_clock, noop_rule_engine) -> None:
    ingestor = EventIngestor(empty_core_db.writer, empty_core_db.repositories, fake_clock, noop_rule_engine)
    t0 = datetime(2026, 8, 12, tzinfo=timezone.utc)
    ingestor.handle(make_envelope(raw=b"1024", at=t0))
    jump = ingestor.handle(make_envelope(raw=b"1433.7", at=t0 + timedelta(minutes=5)))
    assert jump.decode_status == "QUARANTINED"
    assert empty_core_db.scalar("SELECT count(*) FROM measurements") == 1

def test_retained_event_is_audited_without_activity(core_db, make_envelope, fake_clock, noop_rule_engine) -> None:
    ingestor = EventIngestor(core_db.writer, core_db.repositories, fake_clock, noop_rule_engine)
    ingestor.handle(make_envelope(raw=b"1024", retain=True))
    assert core_db.scalar("SELECT count(*) FROM raw_events") == 1
    assert core_db.scalar("SELECT count(*) FROM measurements") == 0

def test_unseen_dup_is_processed_but_committed_redelivery_is_audit_only(core_db, make_envelope, fake_clock, noop_rule_engine) -> None:
    ingestor = EventIngestor(core_db.writer, core_db.repositories, fake_clock, noop_rule_engine)
    unseen = make_envelope(raw=b"1024", duplicate=True, client_id="core", session_epoch=4, packet_id=7)
    ingestor.handle(unseen)       # primeira cópia vista localmente, embora DUP=1
    ingestor.handle(unseen)       # mesma entrega já commitada
    assert core_db.scalar("SELECT count(*) FROM raw_events") == 2
    assert core_db.scalar("SELECT count(*) FROM measurements") == 1
    assert core_db.scalar("SELECT redelivery_count FROM mqtt_deliveries") == 1

def test_dup_after_rolled_back_first_delivery_is_not_lost(core_db, make_envelope, fake_clock, noop_rule_engine) -> None:
    ingestor = EventIngestor(core_db.writer, core_db.repositories, fake_clock, noop_rule_engine)
    first = make_envelope(raw=b"1024", duplicate=False, client_id="core", session_epoch=4, packet_id=9)
    core_db.writer.fail_once_after_raw_insert()
    with pytest.raises(InjectedWriteFailure):
        ingestor.handle(first)
    ingestor.handle(replace(first, duplicate=True))
    assert core_db.scalar("SELECT count(*) FROM raw_events") == 1
    assert core_db.scalar("SELECT count(*) FROM measurements") == 1

def test_packet_id_reuse_same_payload_survives_new_delivery_rollback(core_db, make_envelope, fake_clock, noop_rule_engine) -> None:
    ingestor = EventIngestor(core_db.writer, core_db.repositories, fake_clock, noop_rule_engine)
    first = make_envelope(raw=b"1024", at=T0, duplicate=False, client_id="core", session_epoch=4, packet_id=11)
    ingestor.handle(first)
    reused = replace(first, received_at=T0 + timedelta(minutes=5), duplicate=False)
    core_db.writer.fail_once_after_raw_insert()
    with pytest.raises(InjectedWriteFailure):
        ingestor.handle(reused)
    ingestor.handle(replace(reused, received_at=T0 + timedelta(minutes=6), duplicate=True))
    assert core_db.scalar("SELECT count(*) FROM measurements") == 2
    assert core_db.scalar("SELECT delivery_generation FROM mqtt_deliveries WHERE packet_id=11") == 2
```

- [ ] **Step 2: Executar testes e confirmar que falham**

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_event_ingestion.py -v`

Expected: FAIL porque `EventIngestor` não existe.

- [ ] **Step 3: Implementar intenção durável e uma única transação de efeitos por mensagem**

```python
# src/agente_mqtt/application.py
class EventIngestor:
    def __init__(self, writer: DatabaseWriter, repositories: Repositories,
                 clock: Clock, rule_engine: RuleEngine) -> None:
        self._writer = writer
        self._repos = repositories
        self._clock = clock
        self._rule_engine = rule_engine

    def handle(self, envelope: MqttEnvelope) -> PersistedEvent:
        decoded = decode_payload(envelope)
        def prepare(uow: UnitOfWork) -> DeliveryReservation:
            return self._repos.begin_mqtt_delivery(uow, envelope, self._clock.utc_now())
        def apply(uow: UnitOfWork, reservation: DeliveryReservation) -> PersistedEvent:
            previous = self._repos.last_eligible_measurement(uow, reservoir_for_topic(envelope.topic))
            persisted = classify_event(envelope, decoded, previous, self._clock.utc_now())
            persisted = persisted.with_delivery_disposition(reservation.disposition)
            self._repos.insert_raw_event(uow, persisted)
            if reservation.applies_locally and persisted.decode_status == "VALID" and persisted.eligible_for_activity:
                self._repos.insert_measurement(uow, persisted)
            if reservation.applies_locally and not envelope.retain:
                self._rule_engine.ingest(persisted, uow)
            self._repos.finish_mqtt_delivery(uow, reservation, persisted.event_id)
            return persisted
        return self._writer.execute_staged(prepare, apply)
```

`begin_mqtt_delivery` grava uma intenção mínima em transação própria antes dos efeitos. Para `DUP=0`, sempre incrementa `delivery_generation` da chave `(client_id,session_epoch,packet_id,topic)` e deixa a nova geração `RECEIVED`, mesmo quando packet ID e payload foram reutilizados. Se a transação principal abortar, essa intenção sobrevive. Para `DUP=1`, compara o hash com a geração corrente: `COMMITTED` correspondente retorna `ALREADY_COMMITTED`; `RECEIVED` correspondente retoma exatamente essa geração; ausência ou payload divergente abre uma nova geração `RECEIVED`, registra incidente seguro e nunca descarta silenciosamente. `finish_mqtt_delivery` muda a geração esperada para `COMMITTED` na mesma transação de raw/measurement/regras/outbox; para `ALREADY_COMMITTED`, apenas audita e incrementa redelivery nessa transação. O teste obrigatório cobre reutilização do mesmo packet ID **e mesmo payload**, rollback da nova entrega e redelivery. A intenção isolada não contém payload bruto nem efeitos operacionais; raw, measurement, estados, alarmes e outbox continuam atômicos e o ACK só ocorre depois das duas transações concluírem.

`session_epoch` é persistido pelo receiver e só avança quando o broker informa `session_present=false`. A combinação de epoch e geração evita colisão com packet IDs reutilizados e também recuperação após restart. O callback Paho é serial, e `execute_staged` mantém reserva/efeitos consecutivos no writer mesmo diante de scheduler, manutenção ou outro producer concorrente; nenhuma avaliação pode ocupar a fila no meio das duas fases.

`classify_event` deve aplicar o salto somente quando a leitura anterior aceita ocorreu nos 15 minutos anteriores; diferença exatamente `0.4` é aceita. `retain` e `ALREADY_COMMITTED` permanecem em `raw_events`, mas não atualizam atividade, prazo de ausência, sequência de travamento, `measurements` ou regras. `FIRST_LOCAL_COPY` é a única cópia commitada daquela publicação: ela conta normalmente uma vez para nível, atividade, as seis mensagens de STUCK e confirmações de recovery, independentemente de o bit bruto `duplicate` estar ligado. Regras usam `delivery_disposition`, jamais o bit `duplicate` isolado. O `event_id` é UUID v4 local e `persisted_at_utc` vem do `Clock` dentro da transação. O `RuleEngine` é injetado no ingestor a partir da Task 8; até lá, a fixture usa um `NoOpRuleEngine`, preservando a mesma fronteira transacional sem antecipar regras.

- [ ] **Step 4: Executar testes e consultar invariantes SQL**

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_event_ingestion.py -v`

Expected: PASS; toda `measurement.event_id` referencia exatamente um `raw_event` válido.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/application.py src/agente_mqtt/storage/repositories.py tests/integration/test_event_ingestion.py tests/conftest.py
git commit -m "feat: persist and quarantine MQTT events"
```

### Task 5: Tendência Theil–Sen e projeção de 15 minutos

**Files:**
- Create: `src/agente_mqtt/domain/trend.py`
- Create: `tests/unit/test_trend.py`

**Interfaces:**
- Consumes: medições com `pressure_bar` e `received_at` da Task 4.
- Produces: `estimate_trend(samples, as_of) -> TrendEstimate` e `projects_crossing(estimate, target_bar, within_minutes=15) -> bool`.

- [ ] **Step 1: Escrever testes do estimador puro**

```python
# tests/unit/test_trend.py
from datetime import datetime, timedelta, timezone
import pytest
from agente_mqtt.domain.trend import TimedPressure, estimate_trend, projects_crossing

BASE = datetime(2026, 8, 12, tzinfo=timezone.utc)

def sample(minutes: int, bar: float) -> TimedPressure:
    return TimedPressure(BASE + timedelta(minutes=minutes), bar)

def test_theil_sen_requires_three_samples_spanning_ten_minutes() -> None:
    assert estimate_trend([sample(0, 1.1), sample(5, 1.08)], BASE + timedelta(minutes=5)).slope_bar_per_minute is None
    result = estimate_trend([sample(0, 1.1), sample(5, 1.08), sample(10, 1.06)], BASE + timedelta(minutes=10))
    assert result.slope_bar_per_minute == pytest.approx(-0.004)

def test_projection_detects_low_crossing_within_fifteen_minutes() -> None:
    result = estimate_trend([sample(0, 1.06), sample(5, 1.04), sample(10, 1.02)], BASE + timedelta(minutes=10))
    assert projects_crossing(result, target_bar=1.0, within_minutes=15) is True

def test_equal_timestamps_do_not_divide_by_zero() -> None:
    result = estimate_trend([sample(0, 1.0), sample(0, 1.1), sample(10, 1.2)], BASE + timedelta(minutes=10))
    assert result.slope_bar_per_minute is not None

def test_samples_after_as_of_do_not_change_catchup_trend() -> None:
    samples = [sample(0, 1.10), sample(5, 1.08), sample(10, 1.06), sample(20, 2.00)]
    result = estimate_trend(samples, BASE + timedelta(minutes=10))
    assert result.slope_bar_per_minute == pytest.approx(-0.004)

def test_crossing_horizon_is_measured_from_as_of_not_last_sample() -> None:
    result = estimate_trend(
        [sample(0, 1.20), sample(5, 1.15), sample(10, 1.10)],
        BASE + timedelta(minutes=20),
    )
    assert projects_crossing(result, target_bar=1.0, within_minutes=15) is True
```

- [ ] **Step 2: Executar teste e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_trend.py -v`

Expected: FAIL por módulo ausente.

- [ ] **Step 3: Implementar mediana das inclinações entre pares**

```python
# src/agente_mqtt/domain/trend.py
from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import median

@dataclass(frozen=True)
class TimedPressure:
    received_at: datetime
    pressure_bar: float

@dataclass(frozen=True)
class TrendEstimate:
    last: TimedPressure | None
    slope_bar_per_minute: float | None
    as_of: datetime

def estimate_trend(samples: list[TimedPressure], as_of: datetime) -> TrendEstimate:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    ordered = sorted((item for item in samples if item.received_at <= as_of), key=lambda item: item.received_at)
    if len(ordered) < 3 or (ordered[-1].received_at - ordered[0].received_at).total_seconds() < 600:
        return TrendEstimate(ordered[-1] if ordered else None, None, as_of)
    slopes: list[float] = []
    for left_index, left in enumerate(ordered):
        for right in ordered[left_index + 1:]:
            minutes = (right.received_at - left.received_at).total_seconds() / 60
            if minutes > 0:
                slopes.append((right.pressure_bar - left.pressure_bar) / minutes)
    return TrendEstimate(ordered[-1], median(slopes) if slopes else None, as_of)

def projects_crossing(estimate: TrendEstimate, target_bar: float, within_minutes: float = 15) -> bool:
    if estimate.last is None or estimate.slope_bar_per_minute in (None, 0.0):
        return False
    minutes_from_last = (target_bar - estimate.last.pressure_bar) / estimate.slope_bar_per_minute
    crossing_at = estimate.last.received_at + timedelta(minutes=minutes_from_last)
    eta = (crossing_at - estimate.as_of).total_seconds() / 60
    return 0 <= eta <= within_minutes
```

`estimate_trend` valida também que cada `received_at` é aware; amostras posteriores a `as_of` são ignoradas por definição e nunca entram em catch-up. O lookback recente configurado é aplicado na consulta SQL, sempre com limite superior exclusivo na janela avaliada.

- [ ] **Step 4: Executar testes de tendência**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_trend.py -v`

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/domain/trend.py tests/unit/test_trend.py
git commit -m "feat: estimate reservoir pressure trends"
```

### Task 6: Máquina de qualidade e histórico de transições

**Files:**
- Create: `src/agente_mqtt/domain/quality_machine.py`
- Modify: `src/agente_mqtt/domain/models.py`
- Modify: `src/agente_mqtt/storage/repositories.py`
- Create: `tests/unit/test_quality_machine.py`
- Create: `tests/integration/test_quality_transitions.py`

**Interfaces:**
- Consumes: eventos persistidos e relógio das Tasks 1–4.
- Produces: `QualityState`, `QualityContext`, `QualityDecision`, `decide_quality(context, now)`, projeção `quality_state` e linhas append-only em `state_transitions`.

- [ ] **Step 1: Escrever testes de precedência e instante efetivo**

```python
# tests/unit/test_quality_machine.py
from datetime import datetime, timedelta, timezone
from agente_mqtt.domain.quality_machine import QualityContext, QualityState, decide_quality, missing_severity

T0 = datetime(2026, 8, 12, tzinfo=timezone.utc)

def test_quality_precedence_is_communication_missing_invalid_stuck_good() -> None:
    context = QualityContext(
        current=QualityState.GOOD, activated_at=T0, last_new_message_at=T0,
        disconnected_since=T0, invalid_active=True, stuck_effective_at=T0 + timedelta(minutes=30),
        awaiting_valid_after_reconnect=False,
    )
    decision = decide_quality(context, T0 + timedelta(minutes=31))
    assert decision.new_state is QualityState.COMMUNICATION_LOST
    assert decision.effective_at == T0 + timedelta(minutes=5)

def test_invalid_traffic_prevents_missing_but_stays_invalid() -> None:
    context = QualityContext.good(activated_at=T0, last_new_message_at=T0).with_invalid_traffic(
        received_at=T0 + timedelta(minutes=14)
    )
    assert decide_quality(context, T0 + timedelta(minutes=20)).new_state is QualityState.INVALID

def test_missing_alarm_escalates_at_thirty_minutes() -> None:
    assert missing_severity(T0, T0 + timedelta(minutes=29, seconds=59)) == "WARNING"
    assert missing_severity(T0, T0 + timedelta(minutes=30)) == "CRITICAL"

def test_missing_effective_time_is_threshold_not_evaluation_time() -> None:
    context = QualityContext.good(activated_at=T0, last_new_message_at=T0)
    decision = decide_quality(context, T0 + timedelta(minutes=20))
    assert decision.new_state is QualityState.MISSING
    assert decision.effective_at == T0 + timedelta(minutes=15)

def test_stuck_requires_six_equal_messages_and_thirty_minutes() -> None:
    context = QualityContext.good(activated_at=T0, last_new_message_at=T0).with_equal_sequence(
        value=1024.0, first_at=T0, sixth_at=T0 + timedelta(minutes=25), count=6
    )
    assert decide_quality(context, T0 + timedelta(minutes=29)).new_state is QualityState.GOOD
    decision = decide_quality(context, T0 + timedelta(minutes=30))
    assert decision.new_state is QualityState.STUCK
    assert decision.effective_at == T0 + timedelta(minutes=30)

def test_stuck_effective_time_waits_for_late_sixth_message() -> None:
    context = QualityContext.good(activated_at=T0, last_new_message_at=T0).with_equal_sequence(
        value=1024.0, first_at=T0, sixth_at=T0 + timedelta(minutes=35), count=6
    )
    assert decide_quality(context, T0 + timedelta(minutes=35)).effective_at == T0 + timedelta(minutes=35)

def test_stuck_recovery_needs_two_new_changed_readings_four_minutes_apart() -> None:
    context = QualityContext.stuck(T0).with_changed_reading(1030, T0 + timedelta(minutes=1))
    assert decide_quality(context, T0 + timedelta(minutes=5)).new_state is QualityState.STUCK
    context = context.with_changed_reading(1031, T0 + timedelta(minutes=5))
    assert decide_quality(context, T0 + timedelta(minutes=5)).new_state is QualityState.GOOD

def test_already_committed_redelivery_and_retained_do_not_advance_stuck_recovery() -> None:
    context = QualityContext.stuck(T0).with_changed_reading(1030, T0 + timedelta(minutes=1))
    context = context.with_changed_reading(1031, T0 + timedelta(minutes=6), delivery_disposition="ALREADY_COMMITTED")
    context = context.with_changed_reading(1032, T0 + timedelta(minutes=7), retained=True)
    assert decide_quality(context, T0 + timedelta(minutes=8)).new_state is QualityState.STUCK

def test_first_local_dup_counts_once_for_recovery() -> None:
    context = QualityContext.stuck(T0).with_changed_reading(
        1030, T0 + timedelta(minutes=1), delivery_disposition="FIRST_LOCAL_COPY"
    )
    context = context.with_changed_reading(1031, T0 + timedelta(minutes=5))
    assert decide_quality(context, T0 + timedelta(minutes=5)).new_state is QualityState.GOOD
```

- [ ] **Step 2: Executar testes para confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_quality_machine.py tests/integration/test_quality_transitions.py -v`

Expected: FAIL por classes ausentes.

- [ ] **Step 3: Implementar máquina pura e persistência idempotente**

```python
# src/agente_mqtt/domain/quality_machine.py
class QualityState(StrEnum):
    INITIALIZING = "INITIALIZING"
    GOOD = "GOOD"
    STUCK = "STUCK"
    INVALID = "INVALID"
    MISSING = "MISSING"
    COMMUNICATION_LOST = "COMMUNICATION_LOST"

PRECEDENCE = (
    QualityState.COMMUNICATION_LOST,
    QualityState.MISSING,
    QualityState.INVALID,
    QualityState.STUCK,
    QualityState.GOOD,
    QualityState.INITIALIZING,
)

def decide_quality(context: QualityContext, now: datetime) -> QualityDecision:
    candidates = context.active_candidates(now)
    state = next(
        precedence_state
        for precedence_state in PRECEDENCE
        if any(candidate.state is precedence_state for candidate in candidates)
    )
    chosen = min((c for c in candidates if c.state is state), key=lambda c: c.effective_at)
    return QualityDecision(context.current, state, chosen.effective_at, chosen.trigger_kind)
```

`active_candidates` deve aplicar: comunicação após 5 minutos; missing após 15 minutos desde última mensagem nova ou ativação; invalid enquanto a última condição inválida não foi seguida por leitura aceita; stuck no instante mais tardio entre primeira igual +30 minutos e sexta mensagem; GOOD somente após leitura nova válida na reconexão. Recuperação de `STUCK` exige duas leituras aceitas com valor alterado, separadas por pelo menos 4 minutos. `ALREADY_COMMITTED` e retained nunca contam; `FIRST_LOCAL_COPY` conta exatamente uma vez mesmo quando o bit MQTT `DUP` está ligado. Testes explícitos cobrem as três disposições.

O repositório deve criar no primeiro boot as transições `<none> -> INITIALIZING` e `<none> -> UNKNOWN` e usar fingerprint sobre reservatório, máquina, estado anterior/novo, effective time e rule version. Inserir a transição e atualizar a projeção na mesma transação.

- [ ] **Step 4: Executar testes unitários e de reinício**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_quality_machine.py tests/integration/test_quality_transitions.py -v`

Expected: PASS; reiniciar o repositório não cria outra transição inicial e toda projeção é reconstruível pela última transição.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/domain/quality_machine.py src/agente_mqtt/domain/models.py src/agente_mqtt/storage/repositories.py tests/unit/test_quality_machine.py tests/integration/test_quality_transitions.py
git commit -m "feat: track deterministic measurement quality"
```

### Task 7: Máquina de processo, histerese e confirmação

**Files:**
- Create: `src/agente_mqtt/domain/process_machine.py`
- Create: `tests/unit/test_process_machine.py`

**Interfaces:**
- Consumes: `TrendEstimate`, estado de qualidade, leitura operacional e histórico de confirmações.
- Produces: `ProcessState`, `ProcessContext`, `ProcessDecision`, `decide_process(context) -> ProcessDecision`.

- [ ] **Step 1: Escrever testes dos limites exatos e recuperação**

```python
# tests/unit/test_process_machine.py
from datetime import datetime, timedelta, timezone
import pytest
from agente_mqtt.domain.process_machine import ProcessContext, ProcessState, decide_process
from agente_mqtt.domain.trend import TimedPressure, TrendEstimate

T0 = datetime(2026, 8, 12, tzinfo=timezone.utc)

@pytest.mark.parametrize(("bar", "state"), [(1.0, ProcessState.DRY_RISK), (1.05, ProcessState.LOW_WARNING), (1.15, ProcessState.HIGH_WARNING), (1.2, ProcessState.OVERFLOW_RISK)])
def test_entry_boundaries_are_inclusive(bar: float, state: ProcessState) -> None:
    assert decide_process(ProcessContext.good(current=ProcessState.NORMAL, pressure_bar=bar, received_at=T0)).new_state is state

def test_dry_risk_needs_two_recovery_samples_four_minutes_apart() -> None:
    context = ProcessContext.good(current=ProcessState.DRY_RISK, pressure_bar=1.04, received_at=T0)
    assert decide_process(context).new_state is ProcessState.DRY_RISK
    context = context.with_confirmation(1.04, T0).with_reading(1.04, T0 + timedelta(minutes=4))
    assert decide_process(context).new_state is ProcessState.LOW_WARNING

def test_bad_quality_freezes_process_state() -> None:
    context = ProcessContext.untrusted(current=ProcessState.HIGH_WARNING, pressure_bar=1.0, received_at=T0)
    assert decide_process(context).new_state is ProcessState.HIGH_WARNING

def test_trend_warning_enters_when_crossing_is_within_fifteen_minutes() -> None:
    trend = TrendEstimate(last=TimedPressure(T0, 1.10), slope_bar_per_minute=-0.01, as_of=T0)
    context = ProcessContext.good(
        current=ProcessState.NORMAL, pressure_bar=1.10, received_at=T0, trend=trend
    )
    assert decide_process(context).new_state is ProcessState.LOW_WARNING

@pytest.mark.parametrize(
    ("current", "recovery_bar", "expected"),
    [
        (ProcessState.LOW_WARNING, 1.07, ProcessState.NORMAL),
        (ProcessState.HIGH_WARNING, 1.13, ProcessState.NORMAL),
        (ProcessState.OVERFLOW_RISK, 1.169, ProcessState.HIGH_WARNING),
    ],
)
def test_recovery_boundaries_after_two_confirmations(current, recovery_bar, expected) -> None:
    context = ProcessContext.good(current=current, pressure_bar=recovery_bar, received_at=T0)
    context = context.with_confirmation(recovery_bar, T0).with_reading(
        recovery_bar, T0 + timedelta(minutes=4)
    )
    assert decide_process(context).new_state is expected
```

- [ ] **Step 2: Executar testes para confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_process_machine.py -v`

Expected: FAIL por módulo ausente.

- [ ] **Step 3: Implementar decisão de entrada, tendência e histerese**

```python
# src/agente_mqtt/domain/process_machine.py
def entry_state(pressure_bar: float, trend: TrendEstimate | None) -> ProcessState:
    if pressure_bar <= 1.0:
        return ProcessState.DRY_RISK
    if pressure_bar >= 1.2:
        return ProcessState.OVERFLOW_RISK
    if pressure_bar <= 1.05 or (trend is not None and projects_crossing(trend, 1.0, 15)):
        return ProcessState.LOW_WARNING
    if pressure_bar >= 1.15 or (trend is not None and projects_crossing(trend, 1.2, 15)):
        return ProcessState.HIGH_WARNING
    return ProcessState.NORMAL

def decide_process(context: ProcessContext) -> ProcessDecision:
    if not context.trusted_for_process:
        return ProcessDecision(context.current, context.current, None, "QUALITY_FREEZE")
    candidate = entry_state(context.pressure_bar, context.trend)
    return apply_hysteresis(context, candidate)
```

`apply_hysteresis` deve implementar exatamente: `DRY_RISK >1.03`, `LOW_WARNING >=1.07`, `OVERFLOW_RISK <1.17`, `HIGH_WARNING <=1.13`; duas leituras aceitas consecutivas, separadas por 4 minutos; `ALREADY_COMMITTED` não conta e `FIRST_LOCAL_COPY` conta uma vez; tendência perigosa impede recuperação de warning. Ao reduzir critical, reavaliar warning no mesmo evento. O `effective_at` de entrada por leitura e de recuperação é o `received_at` da leitura que completa a decisão. `RuleEngine.evaluate` consulta somente `received_at_utc < window_end_utc`; catch-up nunca enxerga eventos posteriores à janela.

- [ ] **Step 4: Executar toda a matriz de processo**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_process_machine.py -v`

Expected: PASS, incluindo saltos diretos de NORMAL para estado crítico e troca entre lados somente por leituras confiáveis.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/domain/process_machine.py tests/unit/test_process_machine.py
git commit -m "feat: enforce reservoir process limits and hysteresis"
```

### Task 8: Alarmes determinísticos e outbox atômica

**Files:**
- Create: `src/agente_mqtt/domain/alarms.py`
- Modify: `src/agente_mqtt/storage/repositories.py`
- Modify: `src/agente_mqtt/application.py`
- Create: `tests/integration/test_rule_engine.py`
- Create: `tests/integration/test_alarm_outbox.py`

**Interfaces:**
- Consumes: máquinas das Tasks 6–7 e transação da Task 3.
- Produces: `DeterministicRuleEngine.ingest/evaluate`, `AlarmEvent`, `OutgoingEmail`, alarmes e outbox na mesma transação.

- [ ] **Step 1: Escrever testes de alerta imediato, congelamento e idempotência**

```python
# tests/integration/test_rule_engine.py
import json
from datetime import datetime, timedelta, timezone
import pytest

def test_critical_reading_enqueues_immediately_in_same_transaction(core_app, make_envelope) -> None:
    core_app.ingestor.handle(make_envelope(raw=b"1024"))
    alarm = core_app.db.one("SELECT * FROM alarms WHERE lifecycle='OPEN'")
    mail = core_app.db.one("SELECT * FROM outbox WHERE status='PENDING'")
    assert alarm["kind"] == "DRY_RISK"
    assert mail["alarm_id"] == alarm["alarm_id"]
    assert mail["priority"] == "CRITICAL"

def test_invalid_reading_does_not_clear_process_alarm(core_app, make_envelope) -> None:
    t0 = datetime(2026, 8, 12, tzinfo=timezone.utc)
    core_app.ingestor.handle(make_envelope(raw=b"1024", at=t0))
    core_app.ingestor.handle(make_envelope(raw=b"2000", at=t0 + timedelta(minutes=5)))
    assert core_app.db.scalar("SELECT count(*) FROM alarms WHERE kind='DRY_RISK' AND lifecycle='RECOVERED'") == 0
    active = core_app.active_alarm("0051", "DRY_RISK")
    assert (active.measurement_confirmed, active.confirmation_reason_code) == (False, "QUALITY_INVALID")

@pytest.mark.parametrize("quality", ["MISSING", "INVALID", "STUCK"])
def test_open_process_alarm_and_reminder_show_unconfirmed_quality(core_app, quality) -> None:
    core_app.seed_open_process_alarm("0051", kind="DRY_RISK")
    core_app.force_quality_for_test("0051", quality)
    active = core_app.active_alarm("0051", "DRY_RISK")
    assert active.measurement_confirmed is False
    core_app.enqueue_due_reminder("0051")
    payload = json.loads(core_app.db.one("SELECT payload_json FROM outbox ORDER BY created_at_utc DESC")[0])
    assert payload["measurement_confirmed"] is False
    assert payload["quality_state"] == quality

def test_replaying_same_evaluation_does_not_duplicate_outbox(core_app) -> None:
    boundary = datetime(2026, 8, 12, 0, 15, tzinfo=timezone.utc)
    core_app.seed_last_activity("0051", boundary - timedelta(minutes=15))
    core_app.evaluate("0051", boundary)
    before = {table: core_app.db.scalar(f"SELECT count(*) FROM {table}") for table in (
        "evaluations", "state_transitions", "alarms", "outbox"
    )}
    core_app.restart()
    core_app.evaluate("0051", boundary)
    after = {table: core_app.db.scalar(f"SELECT count(*) FROM {table}") for table in before}
    assert after == before
    for table in ("state_transitions", "alarms", "outbox"):
        assert core_app.db.scalar(f"SELECT count(*)-count(DISTINCT fingerprint) FROM {table}") == 0

def test_warning_reminder_and_recovery_follow_fixed_cadence(core_app, clock) -> None:
    core_app.open_warning("0051", effective_at=clock.now)
    core_app.evaluate("0051", clock.now + timedelta(minutes=59))
    assert core_app.db.scalar("SELECT count(*) FROM outbox WHERE lifecycle='REMINDER'") == 0
    core_app.evaluate("0051", clock.now + timedelta(minutes=60))
    assert core_app.db.scalar("SELECT count(*) FROM outbox WHERE lifecycle='REMINDER'") == 1
    core_app.confirm_recovery("0051", separated_by=timedelta(minutes=4))
    assert core_app.db.scalar("SELECT count(*) FROM outbox WHERE lifecycle='RECOVERY'") == 1

def test_invalid_alarm_escalates_on_second_consecutive_occurrence(core_app, make_envelope) -> None:
    t0 = datetime(2026, 8, 12, tzinfo=timezone.utc)
    core_app.ingestor.handle(make_envelope(raw=b"bad", at=t0))
    assert core_app.latest_alarm("0051", "INVALID").severity == "WARNING"
    core_app.ingestor.handle(make_envelope(raw=b"bad-again", at=t0 + timedelta(minutes=5)))
    alarm = core_app.latest_alarm("0051", "INVALID")
    assert (alarm.severity, alarm.effective_at) == ("CRITICAL", t0 + timedelta(minutes=5))

def test_invalid_alarm_escalates_after_fifteen_minutes_without_valid_reading(core_app, make_envelope) -> None:
    t0 = datetime(2026, 8, 12, tzinfo=timezone.utc)
    core_app.ingestor.handle(make_envelope(raw=b"bad", at=t0))
    core_app.evaluate("0051", t0 + timedelta(minutes=15))
    alarm = core_app.latest_alarm("0051", "INVALID")
    assert (alarm.severity, alarm.effective_at) == ("CRITICAL", t0 + timedelta(minutes=15))

def test_accepted_reading_recovers_invalid_unless_higher_precedence_remains(core_app, make_envelope) -> None:
    t0 = datetime(2026, 8, 12, tzinfo=timezone.utc)
    core_app.ingestor.handle(make_envelope(raw=b"bad", at=t0))
    core_app.ingestor.handle(make_envelope(raw=b"1126.4", at=t0 + timedelta(minutes=5)))
    assert core_app.latest_alarm("0051", "INVALID").lifecycle == "RECOVERED"
```

- [ ] **Step 2: Executar testes e observar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_rule_engine.py tests/integration/test_alarm_outbox.py -v`

Expected: FAIL porque `DeterministicRuleEngine` e alarmes ainda não existem.

- [ ] **Step 3: Implementar orquestração transacional**

```python
# src/agente_mqtt/domain/alarms.py
@dataclass(frozen=True)
class AlarmEvent:
    alarm_id: str
    reservoir_id: str
    kind: str
    severity: str
    lifecycle: str
    effective_at: datetime
    reason_code: str
    trigger_event_id: str | None
    measurement_confirmed: bool | None
    quality_state: str
    confirmation_reason_code: str | None
    fingerprint: str

def email_fingerprint(event: AlarmEvent) -> str:
    return fingerprint("gmail", event.alarm_id, event.lifecycle, event.severity, format_utc(event.effective_at))
```

`DeterministicRuleEngine.ingest` deve atualizar qualidade, processo e alertas imediatos dentro do `UnitOfWork` iniciado pelo `EventIngestor`; nenhuma segunda transação. `evaluate` deve inserir primeiro a chave de avaliação e retornar sem efeitos se houver conflito UNIQUE. A política é: critical/elevação imediata; warning de nível e de ausência na avaliação; primeira invalidade abre WARNING imediatamente; reminder a cada 60 minutos; recovery somente após confirmações. Missing tem severidade warning em 15 minutos e critical em 30. `INVALID` abre WARNING na primeira ocorrência; eleva a CRITICAL no segundo evento inválido consecutivo, com effective time desse evento, ou exatamente 15 minutos após a primeira ocorrência se não houver nova leitura confiável; leitura aceita gera RECOVERED, mas a máquina assume qualquer condição de maior precedência ainda ativa. Cada mudança usa fingerprint por lifecycle/severidade/effective time. Alarmes são append-only em eventos de lifecycle, com projeção atual consultável. Quando a qualidade deixa `GOOD`, cada alarme de processo aberto recebe um evento `STATUS_UPDATED` sem e-mail; `ActiveAlarm` deriva/preserva `measurement_confirmed=false`, `quality_state` e `confirmation_reason_code`. Reminders copiam essa condição para o DTO persistido; só uma leitura confiável pode voltar a confirmação para `true`.

O template persistido na outbox deve conter apenas DTO local, `Message-ID` determinístico e campos objetivos; SMTP não ocorre dentro da transação.

- [ ] **Step 4: Executar testes de atomicidade e replay**

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_rule_engine.py tests/integration/test_alarm_outbox.py -v`

Expected: PASS; uma falha antes do commit deixa zero transição, zero alarme e zero outbox; replay cria zero duplicatas.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/domain/alarms.py src/agente_mqtt/storage/repositories.py src/agente_mqtt/application.py tests/integration/test_rule_engine.py tests/integration/test_alarm_outbox.py
git commit -m "feat: persist deterministic alarms and notifications"
```

### Task 9: Agendador alinhado e recuperação de avaliações

**Files:**
- Create: `src/agente_mqtt/scheduling/scheduler.py`
- Create: `tests/unit/test_scheduler.py`
- Create: `tests/integration/test_evaluation_recovery.py`

**Interfaces:**
- Consumes: `Clock`, `DatabaseWriter`, `DeterministicRuleEngine.evaluate` e chave UNIQUE de avaliação.
- Produces: `next_quarter_hour(now, timezone)`, `due_evaluations(reservoir_id, activated_at, completed_keys, now)`, `EvaluationScheduler.catch_up()` e `run(stop_event)`.

- [ ] **Step 1: Escrever testes de fronteira, timezone e catch-up**

```python
# tests/unit/test_scheduler.py
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from agente_mqtt.scheduling.scheduler import due_evaluations, next_quarter_hour

FORTALEZA = ZoneInfo("America/Fortaleza")

def test_next_boundary_is_strictly_future() -> None:
    now = datetime(2026, 8, 12, 3, 15, tzinfo=timezone.utc)
    assert next_quarter_hour(now, FORTALEZA) == datetime(2026, 8, 12, 3, 30, tzinfo=timezone.utc)

def test_restart_replays_each_missing_boundary_once() -> None:
    activated = datetime(2026, 8, 12, 3, 0, tzinfo=timezone.utc)
    now = datetime(2026, 8, 12, 3, 46, tzinfo=timezone.utc)
    assert due_evaluations("0051", activated, {activated}, now, FORTALEZA) == [
        datetime(2026, 8, 12, 3, 15, tzinfo=timezone.utc),
        datetime(2026, 8, 12, 3, 30, tzinfo=timezone.utc),
        datetime(2026, 8, 12, 3, 45, tzinfo=timezone.utc),
    ]

# tests/integration/test_evaluation_recovery.py
def test_restart_after_only_first_reservoir_committed_finishes_second(scheduler_harness) -> None:
    boundary = datetime(2026, 8, 12, 3, 15, tzinfo=timezone.utc)
    scheduler_harness.activate_at(boundary - timedelta(minutes=15))
    scheduler_harness.fail_after_commit("0051", boundary)
    scheduler_harness.restart_and_catch_up(boundary + timedelta(minutes=1))
    assert scheduler_harness.evaluation_count("0051", boundary) == 1
    assert scheduler_harness.evaluation_count("0022", boundary) == 1

def test_catchup_never_starts_before_activation(scheduler_harness) -> None:
    activated = datetime(2026, 8, 12, 3, 7, tzinfo=timezone.utc)
    scheduler_harness.activate_at(activated)
    scheduler_harness.catch_up(datetime(2026, 8, 12, 3, 31, tzinfo=timezone.utc))
    assert scheduler_harness.boundaries() == [
        ("0051", datetime(2026, 8, 12, 3, 15, tzinfo=timezone.utc)),
        ("0022", datetime(2026, 8, 12, 3, 15, tzinfo=timezone.utc)),
        ("0051", datetime(2026, 8, 12, 3, 30, tzinfo=timezone.utc)),
        ("0022", datetime(2026, 8, 12, 3, 30, tzinfo=timezone.utc)),
    ]
```

- [ ] **Step 2: Executar testes para confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_scheduler.py tests/integration/test_evaluation_recovery.py -v`

Expected: FAIL por scheduler ausente.

- [ ] **Step 3: Implementar cálculo de fronteiras sem sleeps longos**

```python
# src/agente_mqtt/scheduling/scheduler.py
def next_quarter_hour(now_utc: datetime, tz: ZoneInfo) -> datetime:
    local = now_utc.astimezone(tz)
    minute = ((local.minute // 15) + 1) * 15
    boundary = local.replace(second=0, microsecond=0)
    if minute == 60:
        boundary = boundary.replace(minute=0) + timedelta(hours=1)
    else:
        boundary = boundary.replace(minute=minute)
    return boundary.astimezone(timezone.utc)

class EvaluationScheduler:
    def _evaluate(self, reservoir_id: str, window_end: datetime) -> None:
        self._writer.execute(
            lambda uow: self._rule_engine.evaluate(reservoir_id, window_end, uow)
        )

    def catch_up(self) -> None:
        now = self._clock.utc_now()
        for window_end, reservoir_id in self._pending_keys(now):
            self._evaluate(reservoir_id, window_end)

    def run(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            now = self._clock.utc_now()
            for window_end, reservoir_id in self._pending_keys(now):
                self._evaluate(reservoir_id, window_end)
            timeout = max(0.0, (next_quarter_hour(now, self._tz) - now).total_seconds())
            stop_event.wait(min(timeout, 60.0))
```

Persistir UTC; usar timezone apenas para alinhamento. `_pending_keys` faz anti-join por `(reservoir_id, window_end_utc, rule_version)`, ordena por janela e reservatório e limita o início a `activated_at`; nunca usa um cursor global. `_evaluate` é o único adaptador transacional do scheduler: abre `writer.execute(...)` e passa o mesmo `UnitOfWork` exigido pela interface estável do motor. Adicionar teste em que o motor falha após inserir a avaliação e comprovar rollback integral. Mudança regressiva no relógio não duplica devido à chave SQL. `catch_up()` termina de forma síncrona antes de MQTT ser habilitado; o loop periódico só começa depois. Catch-up processa todas as avaliações não registradas desde a ativação dentro da retenção, sem reenviar fingerprints existentes.

- [ ] **Step 4: Executar testes de scheduler e restart**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_scheduler.py tests/integration/test_evaluation_recovery.py -v`

Expected: PASS sem aguardar tempo real, usando `FakeClock` e `threading.Event` fake.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/scheduling tests/unit/test_scheduler.py tests/integration/test_evaluation_recovery.py
git commit -m "feat: schedule idempotent quarter-hour evaluations"
```

### Task 10: Adaptador MQTT TLS, QoS 1 e ACK após commit

**Files:**
- Create: `src/agente_mqtt/mqtt/backoff.py`
- Create: `src/agente_mqtt/mqtt/client.py`
- Create: `src/agente_mqtt/mqtt/session_epoch.py`
- Create: `tests/unit/test_backoff.py`
- Create: `tests/unit/test_mqtt_client.py`
- Create: `tests/integration/test_mqtt_qos1_tls.py`
- Create: `tests/fixtures/mosquitto.conf`

**Interfaces:**
- Consumes: `CoreConfig`, `Clock`, `EventIngestor.handle`, `DatabaseWriter` e repositório de `runtime_health`.
- Produces: `BackoffPolicy.delay(attempt, random_value)`, `SqliteSessionEpochStore`, `MqttReceiver.start/stop`, callbacks Paho v2 e `ConnectionHealth`.

- [ ] **Step 1: Escrever testes da configuração Paho e ordem commit/ACK**

```python
# tests/unit/test_mqtt_client.py
from unittest.mock import Mock
from paho.mqtt import client as mqtt
from agente_mqtt.mqtt.client import MqttReceiver

def test_receiver_subscribes_only_allowlisted_topics_qos_one(receiver_config, epoch_store) -> None:
    paho = Mock(); paho.subscribe.side_effect = [(mqtt.MQTT_ERR_SUCCESS, 101), (mqtt.MQTT_ERR_SUCCESS, 102)]
    receiver = MqttReceiver(receiver_config, handler=Mock(), session_epochs=epoch_store, client_factory=lambda **_: paho)
    receiver.on_connect(paho, None, {}, 0, None)
    assert paho.subscribe.call_args_list == [
        (("EPZ/META/0051/ANI01/RAW", 1),),
        (("EPZ/META/0022/ANI01/RAW", 1),),
    ]
    assert not hasattr(receiver, "publish")

def test_ack_happens_only_after_handler_returns(receiver_config, fake_message, epoch_store) -> None:
    order: list[str] = []
    handler = Mock(); handler.handle.side_effect = lambda _: order.append("committed")
    paho = Mock(); paho.ack.side_effect = lambda *_: order.append("acked")
    receiver = MqttReceiver(receiver_config, handler=handler, session_epochs=epoch_store, client_factory=lambda **_: paho)
    receiver.on_message(paho, None, fake_message)
    assert order == ["committed", "acked"]

def test_handler_failure_does_not_ack_and_marks_fatal(receiver_config, fake_message, epoch_store) -> None:
    handler = Mock(); handler.handle.side_effect = OSError("disk full"); paho = Mock()
    receiver = MqttReceiver(receiver_config, handler=handler, session_epochs=epoch_store, client_factory=lambda **_: paho)
    receiver.on_message(paho, None, fake_message)
    paho.ack.assert_not_called()
    assert receiver.fatal_error is not None

def test_mqtt_v5_connect_uses_nonzero_session_expiry(receiver_config_v5, epoch_store) -> None:
    receiver = MqttReceiver(receiver_config_v5, handler=Mock(), session_epochs=epoch_store, client_factory=recording_client)
    receiver.start()
    assert receiver.client.connect_properties.SessionExpiryInterval == 86_400
    assert receiver.client.clean_start is False

def test_every_connect_resubscribes_idempotently_even_with_session_present(receiver, paho) -> None:
    paho.subscribe.side_effect = [(mqtt.MQTT_ERR_SUCCESS, 101), (mqtt.MQTT_ERR_SUCCESS, 102)]
    receiver.on_connect(paho, None, Mock(session_present=True), 0, None)
    assert paho.subscribe.call_args_list == [
        (("EPZ/META/0051/ANI01/RAW", 1),),
        (("EPZ/META/0022/ANI01/RAW", 1),),
    ]

def test_connection_is_not_ready_until_both_subacks(receiver, paho) -> None:
    paho.subscribe.side_effect = [(mqtt.MQTT_ERR_SUCCESS, 101), (mqtt.MQTT_ERR_SUCCESS, 102)]
    receiver.on_connect(paho, None, Mock(session_present=True), 0, None)
    assert receiver.connection_health.subscription_ready is False
    receiver.on_subscribe(paho, None, 101, [1], None)
    assert receiver.connection_health.subscription_ready is False
    receiver.on_subscribe(paho, None, 102, [1], None)
    assert receiver.connection_health.subscription_ready is True

def test_new_broker_session_advances_durable_epoch(receiver, paho, epoch_store) -> None:
    paho.subscribe.side_effect = [(mqtt.MQTT_ERR_SUCCESS, 101), (mqtt.MQTT_ERR_SUCCESS, 102)]
    before = epoch_store.current()
    receiver.on_connect(paho, None, Mock(session_present=False), 0, None)
    assert epoch_store.current() == before + 1
```

- [ ] **Step 2: Executar os testes e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_backoff.py tests/unit/test_mqtt_client.py -v`

Expected: FAIL por adaptador ausente.

- [ ] **Step 3: Implementar cliente com API de callback v2 e SSLContext seguro**

```python
# src/agente_mqtt/mqtt/client.py
def build_ssl_context(ca_file: Path) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=str(ca_file))
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    return context

def default_client(config: CoreConfig) -> mqtt.Client:
    protocol = mqtt.MQTTv5 if config.mqtt_protocol == "5" else mqtt.MQTTv311
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=config.mqtt_client_id,
        clean_session=False if protocol == mqtt.MQTTv311 else None,
        protocol=protocol,
        manual_ack=True,
    )
    client.tls_set_context(build_ssl_context(config.mqtt_ca_file))
    return client

def connect_persistently(client: mqtt.Client, config: CoreConfig) -> None:
    if config.mqtt_protocol == "5":
        properties = mqtt.Properties(mqtt.PacketTypes.CONNECT)
        properties.SessionExpiryInterval = 86_400
        client.connect(config.mqtt_host, config.mqtt_port,
                       clean_start=False,
                       properties=properties)
    else:
        client.connect(config.mqtt_host, config.mqtt_port)

class SqliteSessionEpochStore:
    def __init__(self, writer: DatabaseWriter, health_repository: RuntimeHealthRepository) -> None:
        self._writer = writer
        self._health = health_repository

    def observe_connect(self, session_present: bool) -> int:
        return self._writer.execute(
            lambda uow: self._health.keep_or_advance_session_epoch(uow, session_present)
        )

    def current(self) -> int:
        return self._health.read_session_epoch()

def on_message(self, client, userdata, message) -> None:
    envelope = MqttEnvelope.from_paho(
        message, self._clock.utc_now(), client_id=self._config.mqtt_client_id,
        session_epoch=self._session_epoch,
    )
    try:
        self._handler.handle(envelope)
    except BaseException as exc:
        self._fatal_error = exc
        self._stop_requested.set()
        client.disconnect()
        return
    client.ack(message.mid, message.qos)
```

O receiver deve usar client ID estável, sessão persistente, `loop_start`, autenticação lida do `SecretProvider`, callbacks de connect/disconnect/subscribe e backoff exponencial com jitter injetável limitado a 60 segundos. Em MQTT 3.1.1 usa `clean_session=False`; em MQTT v5 usa `clean_start=False` inclusive no primeiro connect de cada novo processo e `SessionExpiryInterval=86400`, para retomar a sessão do broker após restart. Em todo CONNACK bem-sucedido envia novamente os dois SUBSCRIBE QoS 1; a operação é idempotente e fecha o crash entre CONNACK e primeira inscrição. Guarda os dois message IDs e só grava/publica `mqtt_subscription_ready=1` depois de ambos os SUBACK aceitarem QoS 1; reconnect/disconnect zera o campo antes da nova negociação, e rejeição desconecta e entra no backoff. `SqliteSessionEpochStore` é injetado explicitamente; ele é o único caminho para `runtime_health` e usa o mesmo `DatabaseWriter`, nunca internals do handler nem conexão escritora paralela. O callback de conexão chama `observe_connect` antes de subscribe: se `session_present=false`, incrementa o epoch no writer; se verdadeiro, mantém o valor após reconnect/restart. O receiver guarda o valor retornado e o coloca em todo `MqttEnvelope`. Qualidade de comunicação exige conexão **e** as duas inscrições confirmadas. Não expor publish. Permitir broker sem TLS somente quando `allow_insecure_local_test=true`, host for loopback e processo não estiver em modo serviço.

- [ ] **Step 4: Rodar testes unitários e integração Mosquitto**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_backoff.py tests/unit/test_mqtt_client.py -v`

Expected: PASS.

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_mqtt_qos1_tls.py -v -m mosquitto --run-mosquitto`

Expected: PASS quando `mosquitto.exe` estiver disponível. O teste força (a) falha/rollback antes do commit seguida de reentrega `DUP=1`, que deve produzir uma medição, e (b) commit bem-sucedido seguido de perda da conexão antes do PUBACK; neste segundo caso reconecta com o mesmo client ID/sessão e recebe o QoS 1 novamente. Deve haver duas linhas de auditoria em `raw_events` (a segunda `duplicate=1`), mas exatamente uma `measurement`, transição lógica, alarme e outbox, sem renovar `last_new_message_at_utc`. Um teste adicional encerra **o processo inteiro** após o commit de `prepare` e antes de `apply/ACK`, publica outra mensagem enquanto ele está parado e inicia um novo processo com o mesmo client ID. Deve receber `session_present=true`, reentregar e aplicar a primeira uma vez, entregar a mensagem enfileirada e confirmar os dois SUBACK. Também verifica retained e avanço do `mqtt_session_epoch` somente quando o broker devolve `session_present=false`.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/mqtt tests/unit/test_backoff.py tests/unit/test_mqtt_client.py tests/integration/test_mqtt_qos1_tls.py tests/fixtures/mosquitto.conf
git commit -m "feat: receive MQTT safely with commit-before-ack"
```

### Task 11: Templates, Gmail SMTP e dispatcher da outbox

**Files:**
- Create: `src/agente_mqtt/notifications/templates.py`
- Create: `src/agente_mqtt/notifications/gmail_smtp.py`
- Create: `src/agente_mqtt/notifications/outbox.py`
- Create: `tests/unit/test_email_templates.py`
- Create: `tests/unit/test_outbox_policy.py`
- Create: `tests/integration/test_outbox_smtp.py`

**Interfaces:**
- Consumes: linhas `outbox`, `SecretProvider`, `Clock`.
- Produces: `render_alarm_email(dto) -> EmailMessage`, `GmailSmtpTransport.send`, `OutboxDispatcher.run/wake`.

- [ ] **Step 1: Escrever testes de conteúdo seguro e lease**

```python
# tests/unit/test_email_templates.py
from dataclasses import replace

def test_critical_email_contains_objective_fields_and_deterministic_message_id(alarm_email_dto) -> None:
    message = render_alarm_email(alarm_email_dto)
    body = message.get_body(preferencelist=("plain",)).get_content()
    assert "0051" in body
    assert "1,000000 bar" in body
    assert "não é comando" in body.lower()
    assert message["Message-ID"] == "<alarm-0051-dry-risk-open@example.invalid>"

def test_email_never_contains_secrets(alarm_email_dto) -> None:
    message = render_alarm_email(alarm_email_dto)
    assert "password" not in message.as_string().lower()

def test_process_alarm_email_marks_level_unconfirmed_when_quality_is_bad(alarm_email_dto) -> None:
    dto = replace(alarm_email_dto, measurement_confirmed=False,
                  quality_state="STUCK", confirmation_reason_code="QUALITY_STUCK")
    body = render_alarm_email(dto).get_body(preferencelist=("plain",)).get_content()
    assert "medição não confirmada" in body.lower()
    assert "STUCK" in body
```

```python
# tests/unit/test_outbox_policy.py
from datetime import timedelta
def test_retry_delay_grows_and_caps_at_sixty_minutes() -> None:
    assert retry_delay(1) == timedelta(minutes=1)
    assert retry_delay(8) == timedelta(minutes=60)
```

- [ ] **Step 2: Executar os testes e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_email_templates.py tests/unit/test_outbox_policy.py tests/integration/test_outbox_smtp.py -v`

Expected: FAIL por módulos ausentes.

- [ ] **Step 3: Implementar SMTP STARTTLS e claim transacional**

```python
# src/agente_mqtt/notifications/gmail_smtp.py
class GmailSmtpTransport:
    def send(self, message: EmailMessage) -> SendReceipt:
        password = self._secrets.get_required(self._credential_target)
        context = ssl.create_default_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        with smtplib.SMTP(self._host, self._port, timeout=30) as smtp:
            smtp.ehlo(); smtp.starttls(context=context); smtp.ehlo()
            smtp.login(self._sender, password)
            refused = smtp.send_message(message)
        if refused:
            raise MailDeliveryError(sorted(refused))
        return SendReceipt(provider_id=message["Message-ID"])
```

`OutboxDispatcher` deve atomizar `claim_due`: trocar `PENDING` por `SENDING`, gravar `lease_owner` e expiração de 5 minutos, commit, enviar fora da transação e então `mark_sent`. Falha reschedule com atraso `min(2**(attempt-1),60)` minutos. Na inicialização, leases expirados voltam a PENDING. Uma linha já `SENT` nunca é reclamada. O renderizador sempre leva `measurement_confirmed`, `quality_state` e motivo do DTO persistido, sem consultar estado mutável depois do claim. Não registrar corpo, destinatários ou credenciais.

- [ ] **Step 4: Rodar testes com SMTP fake**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_email_templates.py tests/unit/test_outbox_policy.py tests/integration/test_outbox_smtp.py -v`

Expected: PASS, incluindo falha temporária, recuperação, lease expirado e fingerprint único.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/notifications tests/unit/test_email_templates.py tests/unit/test_outbox_policy.py tests/integration/test_outbox_smtp.py
git commit -m "feat: deliver alarm emails through persistent outbox"
```

### Task 12: Integridade diária e observabilidade redigida

**Files:**
- Create: `src/agente_mqtt/observability/json_logging.py`
- Create: `src/agente_mqtt/observability/health.py`
- Modify: `src/agente_mqtt/scheduling/scheduler.py`
- Create: `tests/unit/test_health_report.py`
- Create: `tests/unit/test_logging_redaction.py`
- Create: `tests/integration/test_daily_integrity.py`

**Interfaces:**
- Consumes: `runtime_health`, alarmes e outbox.
- Produces: `HealthSnapshot`, `collect_health(conn, now)`, `enqueue_daily_integrity(uow, local_date)` e `RedactingJsonFormatter`.

- [ ] **Step 1: Escrever testes do relatório das 08:00 e redaction**

```python
# tests/unit/test_health_report.py
def test_health_report_exposes_required_operational_counts(health_snapshot) -> None:
    payload = health_snapshot.as_public_dict()
    assert set(payload) == {
        "mqtt_connected", "mqtt_subscription_ready", "reconnection_count", "messages_by_topic",
        "last_message_age_seconds", "last_valid_age_seconds", "invalid_count",
        "last_evaluation_at", "open_alarm_count", "outbox_size",
        "oldest_outbox_age_seconds", "last_email_at",
    }

# tests/unit/test_logging_redaction.py
def test_json_logger_redacts_credentials_and_email(caplog, json_logger) -> None:
    json_logger.info("smtp", extra={"password": "secret", "recipient": "operator@example.com"})
    assert "secret" not in caplog.text
    assert "operator@example.com" not in caplog.text
    assert "[REDACTED]" in caplog.text
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_health_report.py tests/unit/test_logging_redaction.py tests/integration/test_daily_integrity.py -v`

Expected: FAIL por módulos ausentes.

- [ ] **Step 3: Implementar snapshot, e-mail diário e logs JSON rotativos**

```python
# src/agente_mqtt/observability/health.py
def integrity_window(local_date: date, tz: ZoneInfo) -> datetime:
    return datetime.combine(local_date, time(8, 0), tzinfo=tz).astimezone(timezone.utc)

def integrity_fingerprint(local_date: date) -> str:
    return fingerprint("daily-integrity", local_date.isoformat(), "v1")
```

O scheduler deve enfileirar uma integridade por data local na primeira execução em ou após 08:00, protegida por fingerprint UNIQUE. O e-mail deve declarar explicitamente conexão, idades, contagens e alarmes; ausência desse e-mail é o sinal externo de possível indisponibilidade. `RedactingJsonFormatter` aceita allowlist de chaves, converte e-mails e bearer tokens para `[REDACTED]` e usa `RotatingFileHandler`; Event Log recebe somente start/stop/fatal.

- [ ] **Step 4: Executar testes de timezone, replay e redaction**

Run: `.venv\Scripts\python.exe -m pytest tests/unit/test_health_report.py tests/unit/test_logging_redaction.py tests/integration/test_daily_integrity.py -v`

Expected: PASS; duas execuções na mesma data local deixam uma única outbox.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/observability src/agente_mqtt/scheduling/scheduler.py tests/unit/test_health_report.py tests/unit/test_logging_redaction.py tests/integration/test_daily_integrity.py
git commit -m "feat: report MQTT core integrity and health"
```

### Task 13: Retenção e backup SQLite consistente

**Files:**
- Create: `src/agente_mqtt/storage/retention.py`
- Create: `src/agente_mqtt/storage/backup.py`
- Create: `src/agente_mqtt/scheduling/maintenance.py`
- Modify: `src/agente_mqtt/application.py`
- Create: `tests/integration/test_retention_backup.py`
- Create: `tests/integration/test_maintenance_schedule.py`

**Interfaces:**
- Consumes: banco operacional e `Clock`.
- Produces: `apply_retention(uow, now) -> RetentionResult`, `backup_database(source, destination) -> BackupResult`, `prune_backups(directory, keep=30)` e `MaintenanceScheduler` diário idempotente.

- [ ] **Step 1: Escrever testes de prazos e backup durante WAL**

```python
# tests/integration/test_retention_backup.py
from datetime import datetime, timedelta, timezone
import sqlite3

NOW = datetime(2026, 8, 12, tzinfo=timezone.utc)

def test_retention_uses_ninety_and_three_hundred_sixty_five_days(seed_aged_db) -> None:
    result = apply_retention(seed_aged_db.uow, NOW)
    assert result.raw_events_deleted == 1
    assert seed_aged_db.scalar("SELECT count(*) FROM alarms") == 1

def test_backup_is_consistent_while_wal_has_uncheckpointed_data(core_db, tmp_path) -> None:
    target = tmp_path / "backup.sqlite"
    backup_database(core_db.path, target)
    with sqlite3.connect(target) as backup:
        assert backup.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert backup.execute("SELECT count(*) FROM raw_events").fetchone()[0] == core_db.scalar("SELECT count(*) FROM raw_events")

def test_raw_retention_preserves_long_lived_transition_and_alarm(seed_aged_db) -> None:
    apply_retention(seed_aged_db.uow, NOW)
    assert seed_aged_db.scalar("SELECT count(*) FROM raw_events WHERE event_id='old-trigger'") == 0
    assert seed_aged_db.scalar("SELECT count(*) FROM state_transitions WHERE transition_id='t-old'") == 1
    assert seed_aged_db.scalar("SELECT count(*) FROM alarms WHERE alarm_event_id='a-old'") == 1
    assert seed_aged_db.scalar("SELECT trigger_event_id FROM alarms WHERE alarm_event_id='a-old'") is None

def test_transition_retention_keeps_boundary_carry_plus_recent_history(seed_aged_db) -> None:
    seed_aged_db.add_transition("0051", "quality", "GOOD", age_days=500, transition_id="q-older")
    seed_aged_db.add_transition("0051", "quality", "MISSING", age_days=400, transition_id="q-carry")
    seed_aged_db.add_transition("0051", "quality", "GOOD", age_days=1, transition_id="q-recent")
    apply_retention(seed_aged_db.uow, NOW)
    assert seed_aged_db.transition_ids("0051", "quality") == ["q-carry", "q-recent"]

def test_daily_maintenance_runs_once_and_recovers_after_restart(maintenance_harness, now) -> None:
    maintenance_harness.run_due(now)
    maintenance_harness.restart()
    maintenance_harness.run_due(now + timedelta(minutes=1))
    assert maintenance_harness.completed_jobs(local_date=now.date()) == 1
    assert maintenance_harness.completed_backups(local_date=now.date()) == 1
```

- [ ] **Step 2: Executar teste e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_retention_backup.py -v`

Expected: FAIL por módulos ausentes.

- [ ] **Step 3: Implementar limpeza em lotes e `Connection.backup()`**

```python
# src/agente_mqtt/storage/backup.py
def backup_database(source: Path, destination: Path) -> BackupResult:
    temporary = destination.with_suffix(".tmp")
    with open_connection(source, readonly=True) as src, sqlite3.connect(temporary) as dst:
        src.backup(dst, pages=256, sleep=0.01)
        if dst.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise BackupIntegrityError(str(temporary))
    temporary.replace(destination)
    return BackupResult(destination, destination.stat().st_size)
```

`seed_aged_db` deve criar explicitamente `raw_event` e `measurement` antigos, além de `state_transition` e `alarm` ainda dentro da retenção de 365 dias ligados ao mesmo `trigger_event_id`; isso prova `ON DELETE SET NULL` sem depender de fixture vazia. Retenção: raw/measurements 90 dias e alarm events 365 dias. Para cada `(reservoir_id,machine)`, conservar **todas** as `state_transitions` com `effective_at_utc >= cutoff` e também exatamente a última transição com `effective_at_utc < cutoff`; só as anteriores a esse carry são apagadas. Assim uma transição recente dentro de uma janela não elimina o estado que vigorava no início dela. A escolha do carry usa `(effective_at_utc DESC, recorded_at_utc DESC, transition_id DESC)` restrito a `< cutoff` e é testada para ambas as máquinas, inclusive com uma transição recente coexistente. Nunca remover linha referenciada por alarme/outbox pendente. O ledger `mqtt_deliveries` do epoch ativo não é apagado por idade; epochs antigos só são removidos depois de um novo `session_present=false`. Executar deletes em lotes de 1.000 dentro do writer. Manter os 30 backups concluídos mais recentes; ignorar e remover somente temporários pertencentes ao diretório de backup validado. `MaintenanceScheduler` executa diariamente no horário local configurado, usa `maintenance_jobs UNIQUE(local_date, job_version)` criado na migração inicial, faz retenção no writer e backup fora da transação, marcando sucesso somente após integrity check. Falha é repetida com backoff sem interromper MQTT; restart retoma a linha incompleta. A Task 14 inicia e encerra esse scheduler junto dos demais componentes.

- [ ] **Step 4: Executar testes de integridade e retenção**

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_retention_backup.py tests/integration/test_maintenance_schedule.py -v`

Expected: PASS; banco de origem continua recebendo gravações durante backup.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/storage/retention.py src/agente_mqtt/storage/backup.py src/agente_mqtt/scheduling/maintenance.py src/agente_mqtt/application.py tests/integration/test_retention_backup.py tests/integration/test_maintenance_schedule.py
git commit -m "feat: retain and back up operational history"
```

### Task 14: Composição da aplicação e encerramento controlado

**Files:**
- Modify: `src/agente_mqtt/application.py`
- Consume unchanged: `resources/contracts/operational_v1.json`
- Create: `src/agente_mqtt/__main__.py`
- Create: `tests/integration/test_application_lifecycle.py`
- Create: `tests/integration/test_restart_idempotency.py`

**Interfaces:**
- Consumes: todos os componentes das Tasks 1–13.
- Produces: `build_application(config, secrets, clock) -> MqttCoreApplication`, `start()`, `stop(deadline_seconds)` e exit code fatal.

- [ ] **Step 1: Escrever testes de ordem de start/stop e reinício**

```python
# tests/integration/test_application_lifecycle.py
def test_application_starts_writer_before_mqtt_and_stops_mqtt_before_writer(fake_components) -> None:
    app = build_application_from_components(fake_components)
    app.start(); app.stop(deadline_seconds=10)
    assert fake_components.order == [
        "migrate", "writer.start", "initialize_states_and_contract", "scheduler.catch_up",
        "publish_contract_ready", "dispatcher.start", "scheduler.start", "maintenance.start", "mqtt.start",
        "mqtt.stop", "maintenance.stop", "scheduler.stop", "dispatcher.stop", "writer.stop",
    ]

def test_mqtt_cannot_start_while_catchup_is_blocked(fake_components) -> None:
    fake_components.scheduler.block_catch_up()
    start = fake_components.start_application_in_thread()
    fake_components.scheduler.wait_until_catch_up_entered()
    assert "mqtt.start" not in fake_components.order
    assert fake_components.operational_contract_ready == 0
    assert fake_components.runtime_health_core_ready == 0
    fake_components.scheduler.release_catch_up(); start.join(timeout=2)
    assert (fake_components.operational_contract_ready, fake_components.runtime_health_core_ready) == (1, 1)
    assert fake_components.order.index("scheduler.catch_up") < fake_components.order.index("mqtt.start")

def test_fatal_persistence_error_returns_nonzero(fake_components) -> None:
    fake_components.mqtt.fatal_error = OSError("disk unavailable")
    assert run_until_stopped(fake_components.app) == 1

def test_app_exposes_fatal_signal_for_service_wrapper(fake_components) -> None:
    fake_components.app.signal_fatal(OSError("disk unavailable"))
    assert fake_components.app.fatal_event.is_set()
    assert isinstance(fake_components.app.fatal_error, OSError)
```

- [ ] **Step 2: Executar testes e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_application_lifecycle.py tests/integration/test_restart_idempotency.py -v`

Expected: FAIL pela ausência do composition root.

- [ ] **Step 3: Implementar lifecycle explícito**

```python
# src/agente_mqtt/application.py
class MqttCoreApplication:
    @property
    def fatal_event(self) -> threading.Event:
        return self._fatal_event

    @property
    def fatal_error(self) -> BaseException | None:
        return self._fatal_error

    def start(self) -> None:
        self._migrate()
        self._writer.start()
        self._initialize_states_and_contract()
        self._scheduler.catch_up()
        self._publish_contract_ready()
        self._dispatcher.start()
        self._scheduler.start()
        self._maintenance.start()
        self._mqtt.start()

    def stop(self, deadline_seconds: float) -> None:
        deadline = self._clock.monotonic() + deadline_seconds
        self._mqtt.stop()
        self._maintenance.stop(remaining(deadline, self._clock))
        self._scheduler.stop(remaining(deadline, self._clock))
        self._dispatcher.stop(remaining(deadline, self._clock))
        self._writer.stop(remaining(deadline, self._clock))
```

`__main__` deve aceitar somente `run --config ABSOLUTE_PATH` e `validate-config --config ABSOLUTE_PATH`. Config inválida falha antes de abrir rede; fatal de banco encerra com código 1 para o SCM reiniciar; erro Gmail não encerra aquisição. `_initialize_states_and_contract()` valida/grava manifesto e projeções com `ready=0`; `_publish_contract_ready()` só roda após `catch_up()` concluir e muda os dois flags para `1` na mesma transação. Se catch-up falhar ou ficar bloqueado, o Analyst nunca observa prontidão. Somente então iniciam dispatcher, loops e MQTT, impedindo transições históricas fora de ordem. Teste de reinício deve parar após cada fronteira transacional — inclusive entre os dois reservatórios e entre catch-up/ready — e confirmar reconstrução de estados, avaliações e outbox sem nova notificação lógica.

A aplicação observa os sinais fatais do writer e do receiver em uma thread de supervisão curta; o primeiro erro preserva a exceção original, seta `MqttCoreApplication.fatal_event` e inicia shutdown. Falha SMTP permanece não fatal e nunca seta esse evento.

- [ ] **Step 4: Executar testes de lifecycle e a suíte completa**

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_application_lifecycle.py tests/integration/test_restart_idempotency.py -v`

Expected: PASS.

Run: `.venv\Scripts\python.exe -m pytest -q -m "not mosquitto and not soak and not operational and not live"`

Expected: todos os testes não operacionais PASS.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/application.py src/agente_mqtt/__main__.py tests/integration/test_application_lifecycle.py tests/integration/test_restart_idempotency.py
git commit -m "feat: compose resilient MQTT core runtime"
```

### Task 15: Serviço Windows, instalação e segurança operacional

**Files:**
- Create: `src/agente_mqtt/service/paths.py`
- Create: `src/agente_mqtt/service/windows_service.py`
- Create: `scripts/install-mqtt-core.ps1`
- Create: `scripts/uninstall-mqtt-core.ps1`
- Create: `scripts/configure-mqtt-core-recovery.ps1`
- Create: `scripts/verify-mqtt-core.ps1`
- Create: `tests/operational/test_windows_service.py`

**Interfaces:**
- Consumes: `MqttCoreApplication` e caminhos absolutos.
- Produces: serviço `AgenteMqttCore`, delayed auto-start, recovery do SCM, Event Source e smoke test operacional.

- [ ] **Step 1: Escrever teste do wrapper sem instalar serviço**

```python
# tests/operational/test_windows_service.py
import threading
import pytest
from agente_mqtt.service.windows_service import ServiceRuntime

def test_service_stop_reports_pending_until_graceful_shutdown(fake_app, fake_scm) -> None:
    runtime = ServiceRuntime(fake_app, fake_scm)
    fake_app.block_stop_for(seconds=8)
    runtime.start_in_thread()
    runtime.stop()
    assert fake_app.stop_calls == [30.0]
    assert fake_scm.states[0] == "STOP_PENDING"
    assert fake_scm.states[-1] == "STOPPED"
    assert fake_scm.stop_pending_checkpoints_are_strictly_increasing()
    assert fake_scm.maximum_checkpoint_gap_seconds < fake_scm.wait_hint_seconds

def test_service_runtime_reports_failure_when_app_fatal_occurs(fake_app, fake_scm) -> None:
    runtime = ServiceRuntime(fake_app, fake_scm)
    fake_app.signal_fatal(OSError("disk unavailable"))
    assert runtime.run_until_stop_or_fatal() == 1
    assert fake_scm.service_specific_exit_code != 0
    assert fake_scm.states[-1] == "STOPPED"

def test_service_loads_installed_contract_when_cwd_is_system32(installed_service, monkeypatch) -> None:
    monkeypatch.chdir(r"C:\Windows\System32")
    installed_service.start()
    assert installed_service.contract_path == installed_service.config.resource_directory / "contracts" / "operational_v1.json"
    assert installed_service.contract_hash_verified

@pytest.mark.operational
def test_scm_restarts_service_after_fatal_while_running(installed_service) -> None:
    first_pid = installed_service.pid
    installed_service.inject_fatal_after_running(OSError("disk unavailable"))
    assert installed_service.wait_for_new_running_pid(timeout=120) != first_pid
```

- [ ] **Step 2: Executar teste e confirmar a falha**

Run: `.venv\Scripts\python.exe -m pytest tests/operational/test_windows_service.py -v`

Expected: FAIL por wrapper ausente.

- [ ] **Step 3: Implementar `ServiceFramework` e scripts idempotentes**

```python
# src/agente_mqtt/service/windows_service.py
class AgenteMqttCoreService(win32serviceutil.ServiceFramework):
    _svc_name_ = "AgenteMqttCore"
    _svc_display_name_ = "Agente MQTT Core"

    def SvcStop(self) -> None:
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING, waitHint=30000)
        self._stop_event.set()

    def SvcDoRun(self) -> None:
        config_path = Path(win32serviceutil.GetServiceCustomOption(self._svc_name_, "ConfigPath"))
        app = build_application(load_config(config_path), WindowsCredentialProvider(), SystemClock())
        app.start()
        outcome = wait_for_first(self._stop_event, app.fatal_event)
        with PeriodicStopCheckpoint(self._report_stop_pending, interval_seconds=2.0):
            app.stop(deadline_seconds=30.0)
        if outcome is app.fatal_event:
            self.ReportServiceStatus(
                win32service.SERVICE_STOPPED,
                win32ExitCode=win32service.ERROR_SERVICE_SPECIFIC_ERROR,
                svcExitCode=1,
            )
            raise ServiceFatalError() from app.fatal_error
```

O instalador deve exigir elevação, validar caminhos sob `C:\ProgramData\AgenteMQTT`, instalar usando o Python/venv absoluto acessível à conta de serviço, criar Event Source, configurar delayed auto-start, as ações de recovery e `FailureActionsOnNonCrashFailures=1`, mas não inserir credenciais. Resolve a fonte empacotada em `<venv-data>\share\agente-mqtt\contracts\operational_v1.json`, confere seu hash esperado e a copia via arquivo temporário + rename para `<resource_directory>\contracts\operational_v1.json`; aplica ACL de leitura à conta Core e aborta se a fonte/destino/hash não corresponder. O serviço jamais consulta `$PSScriptRoot`, repositório ou CWD em runtime. `PeriodicStopCheckpoint` usa thread/timer independente, inicia antes de `app.stop`, reporta `STOP_PENDING` a cada 2 segundos mesmo se um único componente bloquear, e sempre é encerrado no `finally`; callbacks após `STOPPED` são proibidos. Deve imprimir os nomes exatos das credenciais a provisionar. Aceita opcionalmente o SID da conta `StatisticalAnalyst`: concede somente Read+Traverse ao diretório do banco operacional e aos arquivos SQLite `operational.sqlite`, `-wal` e `-shm`; nunca concede Create/Write/Delete. Sem esse SID, imprime o comando exato que deverá ser executado antes da instalação analítica. O verificador confirma serviço, conta dedicada, ACLs, hash do recurso instalado, TLS, ausência de permissão MQTT publish por teste negativo, BitLocker e reinício real após fatal não-crash; falhar se qualquer pré-condição operacional não estiver comprovada.

- [ ] **Step 4: Rodar teste unitário e smoke test em Windows elevado**

Run: `.venv\Scripts\python.exe -m pytest tests/operational/test_windows_service.py -v --run-operational`

Expected: PASS.

Run (PowerShell elevado, máquina de homologação): `powershell -ExecutionPolicy Bypass -File scripts/verify-mqtt-core.ps1 -ConfigPath C:\ProgramData\AgenteMQTT\config\mqtt-core.toml`

Expected: saída `MQTT_CORE_PREFLIGHT_OK`; serviço inicia, para e reinicia sem perder banco/outbox.

- [ ] **Step 5: Commit**

```powershell
git add src/agente_mqtt/service scripts tests/operational/test_windows_service.py
git commit -m "feat: run MQTT core as hardened Windows service"
```

### Task 16: Gates de aceitação e runbook do piloto

**Files:**
- Create: `tests/integration/test_acceptance_scenarios.py`
- Create: `tests/integration/test_core_24h_soak.py`
- Modify: `tests/conftest.py`
- Create: `docs/runbooks/mqtt-core-installation.md`
- Create: `docs/runbooks/mqtt-core-pilot.md`
- Modify: `config/mqtt-core.example.toml`

**Interfaces:**
- Consumes: serviço completo.
- Produces: suite de aceitação repetível, teste de soak opt-in e instruções operacionais sem segredos.

`tests/conftest.py` já registra desde a Task 1 todos os markers e opções; esta tarefa apenas acrescenta fixtures/harnesses. Por padrão, testes Mosquitto, operacionais, live e soak são ignorados. `--mqtt-host` não habilita sozinho teste destrutivo ou de 24 horas.

- [ ] **Step 1: Codificar a tabela de cenários como testes parametrizados**

```python
# tests/integration/test_acceptance_scenarios.py
@pytest.mark.parametrize(
    ("name", "samples", "evaluate_at_minutes", "expected_quality", "expected_process", "expected_mail"),
    [
        ("normal", [(0, 1.10), (5, 1.10), (10, 1.10)], 10, "GOOD", "NORMAL", None),
        ("low_warning", [(14, 1.05)], 15, "GOOD", "LOW_WARNING", "WARNING"),
        ("dry", [(0, 1.00)], 0, "GOOD", "DRY_RISK", "CRITICAL"),
        ("high_warning", [(14, 1.15)], 15, "GOOD", "HIGH_WARNING", "WARNING"),
        ("overflow", [(0, 1.20)], 0, "GOOD", "OVERFLOW_RISK", "CRITICAL"),
        ("missing", [(0, 1.10)], 15, "MISSING", "NORMAL", "WARNING"),
        ("stuck", [(0, 1.10), (6, 1.10), (12, 1.10), (18, 1.10), (24, 1.10), (30, 1.10), (40, 1.10)], 45, "STUCK", "NORMAL", "WARNING"),
        ("invalid_parse", [(0, "bad")], 0, "INVALID", "UNKNOWN", "WARNING"),
        ("invalid_range", [(0, 4.1)], 0, "INVALID", "UNKNOWN", "WARNING"),
        ("invalid_jump", [(0, 1.10), (5, 1.51)], 5, "INVALID", "NORMAL", "WARNING"),
    ],
)
def test_acceptance_scenario(harness, name, samples, evaluate_at_minutes, expected_quality, expected_process, expected_mail) -> None:
    harness.feed(samples); harness.evaluate(at_minutes=evaluate_at_minutes)
    assert harness.quality == expected_quality
    assert harness.process == expected_process
    assert harness.latest_mail_priority == expected_mail

def test_acceptance_communication_loss_uses_disconnect_origin(harness) -> None:
    harness.feed([(0, 1.10)])
    harness.disconnect(at_minutes=1)
    harness.evaluate(at_minutes=6)
    assert (harness.quality, harness.latest_mail_priority) == ("COMMUNICATION_LOST", "WARNING")

def test_acceptance_recovery_hysteresis_and_nonactivity_messages(harness) -> None:
    harness.prove_all_recoveries_with_two_samples_at_least_four_minutes_apart()
    harness.prove_committed_redelivery_and_retained_do_not_renew_activity_or_confirm_recovery()
    harness.prove_first_local_dup_after_rollback_counts_exactly_once()
    harness.prove_warning_to_critical_escalations_and_reminders()
```

- [ ] **Step 2: Executar a suite completa com cobertura**

Run: `.venv\Scripts\python.exe -m pytest -m "not mosquitto and not soak and not operational and not live" --cov=agente_mqtt --cov-report=term-missing --cov-fail-under=90`

Expected: PASS e cobertura total pelo menos 90%; módulos `domain/*` pelo menos 95% via relatório.

- [ ] **Step 3: Escrever runbooks com comandos concretos**

O runbook de instalação deve conter: instalar CPython 3.12 x64; criar venv em `C:\Program Files\AgenteMQTT\venv`; instalar o wheel/locks; confirmar migration empacotada; copiar e verificar o manifesto para o `resource_directory` absoluto; copiar TOML; provisionar quatro alvos de credencial (`mqtt.username`, `mqtt.password`, `gmail.username`, `gmail.app_password`) na conta do serviço; executar install/recovery/verify a partir de um CWD fora do repositório; consultar Event Log; rollback por uninstall preservando `C:\ProgramData\AgenteMQTT\data` e os recursos versionados da versão anterior.

O runbook do piloto deve conter: confirmação de ACL subscribe-only; teste de cada estado; desligamento/reconexão do broker; falha Gmail; reinício Windows; verificação de integridade 08:00; backup/restore; soak de 24 horas; formulário de evidência e critério de aprovação 9/9 da especificação.

- [ ] **Step 4: Executar soak opt-in e checklist Windows**

Run: `.venv\Scripts\python.exe -m pytest tests/integration/test_core_24h_soak.py -m soak --run-soak --mqtt-host localhost --duration-hours 24`

Expected: 100% das mensagens publicadas nos dois tópicos presentes em `raw_events`, nenhuma notificação lógica duplicada, avaliações completas e nenhuma thread fatal.

Run: `powershell -ExecutionPolicy Bypass -File scripts/verify-mqtt-core.ps1 -ConfigPath C:\ProgramData\AgenteMQTT\config\mqtt-core.toml -RunAlarmMatrix`

Expected: `MQTT_CORE_ACCEPTANCE_OK` e arquivo de evidência sem segredos.

- [ ] **Step 5: Commit**

```powershell
git add tests/conftest.py tests/integration/test_acceptance_scenarios.py tests/integration/test_core_24h_soak.py docs/runbooks config/mqtt-core.example.toml
git commit -m "test: gate MQTT core operational pilot"
```

## Verificação final deste plano

Executar, nesta ordem:

```powershell
.venv\Scripts\python.exe -m pytest -m "not mosquitto and not soak and not operational and not live" --cov=agente_mqtt --cov-report=term-missing --cov-fail-under=90
.venv\Scripts\python.exe -m compileall -q src tests
git diff --check
powershell -ExecutionPolicy Bypass -File scripts/verify-mqtt-core.ps1 -ConfigPath C:\ProgramData\AgenteMQTT\config\mqtt-core.toml -RunAlarmMatrix
```

Critério: comandos com exit code 0, suíte sem falhas, preflight operacional aprovado e nenhuma alteração do Statistical Analyst neste plano.
