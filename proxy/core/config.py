import json
import os

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

# Глобальная константа маркера разделителя HTTP-заголовков
HTTP_HEADER_DELIMITER = b"\r\n\r\n"


class UpstreamConfig(BaseModel):
    """Конфигурация одного целевого апстрим-сервера."""
    host: str
    port: int


class TimeoutConfig(BaseModel):
    """Таймауты сессии из config.json. Автоматически переводит миллисекунды в секунды."""
    connect_ms: float
    read_ms: float
    write_ms: float
    total_ms: float

    # Эти свойства позволяют серверу прозрачно использовать секунды в коде сокетов
    @property
    def connect(self) -> float:
        return self.connect_ms / 1000.0

    @property
    def read(self) -> float:
        return self.read_ms / 1000.0

    @property
    def write(self) -> float:
        return self.write_ms / 1000.0

    @property
    def total(self) -> float:
        return self.total_ms / 1000.0


class LimitConfig(BaseModel):
    """Ограничения на максимальное количество одновременных TCP-соединений."""
    max_client_conns: int
    max_conns_per_upstream: int
    metrics_interval: float


class LoggingConfig(BaseModel):
    """Конфигурация уровня логирования."""
    level: str


class ProxySettings(BaseSettings):
    """Менеджер конфигурации сервера. Строго валидирует структуру с миллисекундами."""
    listen: str
    upstreams: list[UpstreamConfig]
    timeouts: TimeoutConfig
    limits: LimitConfig
    logging: LoggingConfig

    @property
    def listen_host(self) -> str:
        return self.listen.split(":", 1)[0].strip() if ":" in self.listen else "127.0.0.1"

    @property
    def listen_port(self) -> int:
        return int(self.listen.split(":", 1)[1]) if ":" in self.listen else 8080

    @property
    def log_level(self) -> str:
        return self.logging.level.upper()


# Синглтон изначально пустой, он наполнится строго при чтении файла
settings: ProxySettings = None  # type: ignore


def load_settings_from_file(path: str) -> None:
    """Глобальная функция обновления синглтона из JSON файла."""
    global settings
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            config_data = json.load(f)
            settings = ProxySettings.model_validate(config_data)
