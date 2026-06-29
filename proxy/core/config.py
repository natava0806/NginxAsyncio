import json
import os

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


# Глобальная константа маркера разделителя HTTP-заголовков
HTTP_HEADER_DELIMITER = b"\r\n\r\n"


class UpstreamConfig(BaseModel):
    """Конфигурация одного целевого апстрим-сервера."""
    host: str = "127.0.0.1"
    port: int = 9001


class TimeoutConfig(BaseModel):
    """Таймауты сессии. Автоматически переводит миллисекунды в секунды."""
    connect_ms: float = 1000.0
    read_ms: float = 15000.0
    write_ms: float = 15000.0
    total_ms: float = 30000.0

    @property
    def connect(self) -> float: return self.connect_ms / 1000.0

    @property
    def read(self) -> float: return self.read_ms / 1000.0

    @property
    def write(self) -> float: return self.write_ms / 1000.0

    @property
    def total(self) -> float: return self.total_ms / 1000.0


class LimitConfig(BaseModel):
    """Ограничения на максимальное количество одновременных TCP-соединений."""
    max_client_conns: int = 1000
    max_conns_per_upstream: int = 100


class LoggingConfig(BaseModel):
    """Конфигурация уровня логирования."""
    level: str = "info"


class ProxySettings(BaseSettings):
    """Глобальный менеджер конфигурации сервера на базе Pydantic Settings."""
    listen: str = "127.0.0.1:8080"
    upstreams: list[UpstreamConfig] = Field(default_factory=lambda: [
        UpstreamConfig(host="127.0.0.1", port=9001),
        UpstreamConfig(host="127.0.0.1", port=9002)
    ])
    timeouts: TimeoutConfig = Field(default_factory=TimeoutConfig)
    limits: LimitConfig = Field(default_factory=LimitConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @property
    def listen_host(self) -> str:
        return self.listen.split(":", 1)[0].strip() if ":" in self.listen else "127.0.0.1"

    @property
    def listen_port(self) -> int:
        return int(self.listen.split(":", 1)[1]) if ":" in self.listen else 8080

    @property
    def log_level(self) -> str:
        return self.logging.level.upper()


# Инициализируем синглтон. По умолчанию Pydantic подтянет дефолты из классов!
settings = ProxySettings()

def load_settings_from_file(path: str) -> None:
    """Глобальная функция обновления синглтона из JSON файла."""
    global settings
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            config_data = json.load(f)
            settings = ProxySettings.model_validate(config_data)
