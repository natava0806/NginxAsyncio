import asyncio
import os
import sys


# Автоматическое добавление корневой директории проекта.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proxy.core.logger import logger
from proxy.transport.proxy_server import ProxyServer


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config.json")

    # Если файла конфигурации нет, генерируем его НАПРЯМУЮ из дефолтов Pydantic!
    if not os.path.exists(config_path):
        from proxy.core.config import ProxySettings
        # model_dump_json автоматически сериализует все дефолты в красивый JSON
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(ProxySettings().model_dump_json(indent=4))

    # Импортируем нашу функцию явной загрузки
    from proxy.core.config import load_settings_from_file
    load_settings_from_file(config_path)

    # Создание экземпляра асинхронного Nginx и его запуск
    proxy = ProxyServer()
    try:
        asyncio.run(proxy.start())
    except KeyboardInterrupt:
        logger.info("Mini-Nginx stopped by user.")


if __name__ == "__main__":
    main()
