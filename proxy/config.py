import json
import os
from dataclasses import dataclass, field
from typing import List, Dict, Any


@dataclass(frozen=True)
class UpstreamConfig:
    """Конфигурация одного целевого апстрим-сервера."""
    host: str
    port: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UpstreamConfig":
        return cls(
            host=str(data.get("host", "127.0.0.1")),
            port=int(data.get("port", 9001))
        )


@dataclass(frozen=True)
class TimeoutConfig:
    """Таймауты сессии. Переводит миллисекунды из файла в секунды для asyncio."""
    connect: float
    read: float
    write: float
    total: float

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TimeoutConfig":
        return cls(
            connect=float(data.get("connect_ms", 1000)) / 1000.0,
            read=float(data.get("read_ms", 15000)) / 1000.0,
            write=float(data.get("write_ms", 15000)) / 1000.0,
            total=float(data.get("total_ms", 30000)) / 1000.0
        )


@dataclass(frozen=True)
class LimitConfig:
    """Ограничения на максимальное количество одновременных TCP-соединений."""
    max_client_conns: int
    max_conns_per_upstream: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LimitConfig":
        return cls(
            max_client_conns=int(data.get("max_client_conns", 1000)),
            max_conns_per_upstream=int(data.get("max_conns_per_upstream", 100))
        )


class ProxySettings:
    """Глобальный менеджер конфигурации сервера (Синглтон)."""

    def __init__(self):
        # Резервные дефолтные значения
        self.listen_host: str = "127.0.0.1"
        self.listen_port: int = 8080
        self.upstreams: List[UpstreamConfig] = [
            UpstreamConfig("127.0.0.1", 9001),
            UpstreamConfig("127.0.0.1", 9002)
        ]
        self.timeouts: TimeoutConfig = TimeoutConfig(connect=1.0, read=15.0, write=15.0, total=30.0)
        self.limits: LimitConfig = LimitConfig(max_client_conns=1000, max_conns_per_upstream=100)
        self.log_level: str = "INFO"

    def load_from_file(self, path: str) -> None:
        """Разбор JSON конфигурации с валидацией через фабрики."""
        if not os.path.exists(path):
            return

        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Разбор строки вида "ip:port"
        listen = data.get("listen", f"{self.listen_host}:{self.listen_port}")
        if ":" in listen:
            host, port = listen.split(":", 1)
            self.listen_host = host.strip()
            self.listen_port = int(port)

        # Мапп структуры через фабричные методы классов
        if "upstreams" in data and isinstance(data["upstreams"], list):
            self.upstreams = [UpstreamConfig.from_dict(u) for u in data["upstreams"]]

        self.timeouts = TimeoutConfig.from_dict(data.get("timeouts", {}))
        self.limits = LimitConfig.from_dict(data.get("limits", {}))
        self.log_level = data.get("logging", {}).get("level", "INFO").upper()


# Экспорт единственного глобального синглтона
settings = ProxySettings()
