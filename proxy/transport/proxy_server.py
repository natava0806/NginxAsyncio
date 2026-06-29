import asyncio

from proxy.core.config import settings
from proxy.core.logger import logger
from proxy.core.metrics import metrics
from proxy.services.client_handler import ClientConnectionHandler
from proxy.services.upstream_pool import UpstreamPool


class ProxyServer:
    """TCP-сервер оркестрации, управляющий входящим трафиком и ресурсами."""

    def __init__(self):
        # Настройка глобального логгера на уровень из конфига
        # ИСПРАВЛЕНО: Убрали setup_logger(), используем настроенный logger
        # setup_logger(settings.log_level)

        # Инициализация пула апстримов и семафора глобальных клиентских лимитов
        self._pool = UpstreamPool(settings.upstreams, settings.limits.max_conns_per_upstream)
        self._queue = asyncio.Queue(maxsize=settings.limits.max_client_conns)
        self._server = None

        # Хранилище сильных ссылок на задачи (защита от сборщика мусора GC)
        self._background_tasks = set()

        # Пул из 50 постоянных воркеров для разбора очереди
        self._num_workers = 50

    async def _worker_loop(self) -> None:
        """Постоянный воркер, который выгребает и обрабатывает клиентов из очереди."""
        while True:
            # Ожидание появления нового клиента в очереди
            reader, writer = await self._queue.get()
            try:
                handler = ClientConnectionHandler(reader, writer, self._pool)
                await handler.handle()
            except Exception as err:
                logger.error(f"Error inside proxy worker processing pipeline: {err}")
            finally:
                # Обязательно сигнализировать очереди, что задача закрыта
                self._queue.task_done()

    async def _dispatch_client(self, reader, writer):
        """Обработка нового клиента с защитой по количеству подключений."""
        if self._queue.full():
            logger.warning("Global client limits exceeded! Drop incoming connection.")
            try:
                writer.write(b"HTTP/1.1 503 Service Unavailable\r\n\r\n")
                await writer.drain()
                writer.close()
            except Exception as err:
                logger.error(f"Failed to send 503 response to rejected client: {err}")
            return

        # Атомарно пушим кортеж стримов в очередь без блокировки Event Loop
        self._queue.put_nowait((reader, writer))

    # async def _run_handler(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    #    """Оборачивает хэндлер в семафор контроля емкости прокси."""
    #    async with self._global_client_semaphore:
    #        handler = ClientConnectionHandler(reader, writer, self._pool)
    #        await handler.handle()

    async def _log_metrics_loop(self) -> None:
        """Фоновый цикл метрик [INDEX]."""
        while True:
            await asyncio.sleep(settings.limits.metrics_interval)
            stats = metrics.get_stats()
            logger.info(
                f"[METRICS] Active Conns: {stats['active_connections']} | "
                f"Queue Size: {self._queue.qsize()} | "
                f"RPS: {stats['current_rps']} | "
                f"Total Req: {stats['total_requests']} | "
                f"Errors: {stats['total_errors']}"
            )

    async def start(self) -> None:
        """Запускает пул воркеров, TCP-сервер и фоновый поток мониторинга метрик."""
        # 1. Запуск пула фоновых воркеров для разбора очереди
        for _ in range(self._num_workers):
            worker_task = asyncio.create_task(self._worker_loop())
            self._background_tasks.add(worker_task)
            worker_task.add_done_callback(self._background_tasks.discard)

        # 2. Старт сетевого TCP-сервера
        self._server = await asyncio.start_server(
            self._dispatch_client,
            settings.listen_host,
            settings.listen_port,
            limit=65536
        )

        addr = self._server.sockets[0].getsockname()
        logger.info(f"High-Performance Mini-Nginx running on http://{addr[0]}:{addr[1]}")  # noqa: S113

        # 3. Запуск фоновой задачи периодического вывода статистики
        metrics_task = asyncio.create_task(self._log_metrics_loop())
        self._background_tasks.add(metrics_task)
        metrics_task.add_done_callback(self._background_tasks.discard)

        async with self._server:
            await self._server.serve_forever()
