import asyncio
import json
import os
import sys

# Автоматическое добавление корневой директории проекта.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from proxy.core.config import ProxySettings, load_settings_from_file
from proxy.core.logger import logger
from proxy.transport.proxy_server import ProxyServer


def main() -> None:
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, "config.json")

    if not os.path.exists(config_path):
        print(f"Configuration file strictly required but missing: {config_path}")
        return

    load_settings_from_file(config_path)

    with open(config_path, encoding="utf-8") as f:
        config_data = json.load(f)
        app_settings = ProxySettings.model_validate(config_data)

    # Динамически выставляем уровень логирования из конфига
    logger.setLevel(app_settings.log_level)

    proxy = ProxyServer(app_settings)

    try:
        asyncio.run(proxy.start())
    except KeyboardInterrupt:
        logger.info("Async Mini-Nginx stopped by user.")


if __name__ == "__main__":
    main()