import asyncio
import sys
import os

# Автоматическое добавление корневой директории проекта.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proxy.proxy_server import ProxyServer
from proxy.config import settings


def main():
    config_path = "config.json"

    # Если файла config.json нет рядом, автоматически нужно создать дефолтный
    # с параметрами строго из ТЗ.
    if not os.path.exists(config_path):
        import json
        default_config = {
            "listen": "127.0.0.1:8080",
            "upstreams": [
                {"host": "127.0.0.1", "port": 9001},
                {"host": "127.0.0.1", "port": 9002}
            ],
            "timeouts": {
                "connect_ms": 1000,
                "read_ms": 15000,
                "write_ms": 15000,
                "total_ms": 30000
            },
            "limits": {
                "max_client_conns": 1000,
                "max_conns_per_upstream": 100
            },
            "logging": {
                "level": "info"
            }
        }
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(default_config, f, indent=4)

    # Загрузка настроек из файла в глобальный синглтон settings
    settings.load_from_file(config_path)

    # Создание экземпляра асинхронного Nginx и его запуск
    proxy = ProxyServer()
    try:
        asyncio.run(proxy.start())
    except KeyboardInterrupt:
        # Корректный перехват Ctrl+C в терминале для завершения работы без простыни ошибок
        print("\n[INFO] Mini-Nginx stopped by user.")


if __name__ == "__main__":
    main()
