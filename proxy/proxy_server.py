import asyncio
from proxy.config import settings
from proxy.logger import logger, setup_logger
from proxy.upstream_pool import UpstreamPool
from proxy.client_handler import ClientConnectionHandler
from proxy.metrics import metrics


class ProxyServer:
    """TCP-сервер оркестрации, управляющий входящим трафиком и ресурсами."""

    def __init__(self):
        # Настройка глобального логгера на уровень из конфига
        setup_logger(settings.log_level)

        # Инициализация пула апстримов и семафора глобальных клиентских лимитов
        self.pool = UpstreamPool(settings.upstreams, settings.limits.max_conns_per_upstream)
        self.global_client_semaphore = asyncio.Semaphore(settings.limits.max_client_conns)
        self.server = None

        # Хранилище сильных ссылок на задачи (защита от сборщика мусора GC)
        self._background_tasks = set()

    async def start(self) -> None:
        """Запускает TCP-сервер и фоновый поток мониторинга метрик."""
        # Фиксация размера буфера сокета чтения (limit=65536) для стабильного потребления RAM
        self.server = await asyncio.start_server(
            self._dispatch_client,
            settings.listen_host,
            settings.listen_port,
            limit=65536
        )

        addr = self.server.sockets[0].getsockname()
        logger.info(f"High-Performance Mini-Nginx running on http://{addr[0]}:{addr[1]}")

        # Запуск фоновой задачи периодического вывода статистики
        metrics_task = asyncio.create_task(self._log_metrics_loop())
        self._background_tasks.add(metrics_task)
        metrics_task.add_done_callback(self._background_tasks.discard)

        async with self.server:
            await self.server.serve_forever()

    async def _dispatch_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Запускает конкурентную задачу обработки для каждого нового клиента."""
        # Стратегия Load Shedding: если лимит сервера исчерпан, сразу сбрасываем клиента (почитать побольше про это!!!)
        if self.global_client_semaphore.locked():
            logger.warning("Global client limits exceeded! Drop incoming connection.")
            try:
                writer.write(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\nServer Busy")
                await writer.drain()
                writer.close()
            except:
                pass
            return

        # Создание асинхронной задачи обработки. Сохранение в set() от GC.
        task = asyncio.create_task(self._run_handler(reader, writer))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _run_handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Оборачивает хэндлер в семафор контроля емкости прокси."""
        async with self.global_client_semaphore:
            handler = ClientConnectionHandler(reader, writer, self.pool)
            await handler.handle()

    async def _log_metrics_loop(self) -> None:
        """Фоновый бесконечный цикл вывода операционных метрик в лог."""
        while True:
            await asyncio.sleep(5)
            stats = metrics.get_stats()
            logger.info(
                f"[METRICS] Active Conns: {stats['active_connections']} | "
                f"RPS: {stats['current_rps']} | "
                f"Total Req: {stats['total_requests']} | "
                f"Errors: {stats['total_errors']}"
            )
