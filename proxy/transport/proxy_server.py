import asyncio

from proxy.core.config import ProxySettings
from proxy.core.logger import logger
from proxy.core.metrics import metrics
from proxy.services.client_handler import ClientConnectionHandler
from proxy.services.upstream_pool import UpstreamPool


class ProxyServer:
    """Асинхронный TCP-сервер оркестрации, управляющий входящим трафиком через пул воркеров."""

    def __init__(self, app_settings: ProxySettings) -> None:
        # Внедряем настройки через Dependency Injection
        self._settings = app_settings

        # Передаем оригинальный список моделей Pydantic, так как UpstreamPool сам распакует их!
        self._pool = UpstreamPool(
            self._settings.upstreams,
            self._settings.limits.max_conns_per_upstream
        )
        self._queue = asyncio.Queue(maxsize=self._settings.limits.max_client_conns)
        self._server = None

        # Хранилище сильных ссылок на задачи (защита от сборщика мусора GC)
        self._background_tasks = set()

        # Пул из 50 постоянных асинхронных воркеров для разбора очереди
        self._num_workers = 50

    async def _worker_loop(self) -> None:
        """Постоянный воркер, который выгребает и обрабатывает клиентов из очереди."""
        while True:
            # Ожидание появления нового клиента в очереди
            reader, writer = await self._queue.get()
            try:
                handler = ClientConnectionHandler(reader, writer, self._pool, self._settings)
                await handler.handle()
            except Exception as err:
                logger.error(f"Error inside proxy worker processing pipeline: {err}")
            finally:
                # Обязательно сигнализировать очереди, что задача закрыта
                self._queue.task_done()

    async def _dispatch_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Обработка нового клиента с защитой по количеству подключений (Load Shedding)."""
        if self._queue.full():
            logger.warning("Global client limits exceeded! Drop incoming connection.")
            try:
                writer.write(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\nServer Busy")
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except Exception as err:
                logger.error(f"Failed to send 503 response to rejected client: {err}")
            return

        # Атомарно пушим кортеж асинхронных стримов в очередь без блокировки Event Loop
        self._queue.put_nowait((reader, writer))

    async def _log_metrics_loop(self) -> None:
        """Фоновый цикл периодического вывода статистики."""
        while True:
            await asyncio.sleep(self._settings.limits.metrics_interval)
            stats = metrics.get_stats()
            logger.info(
                f"[METRICS] Active Conns: {stats['active_connections']} | "
                f"Queue Size: {self._queue.qsize()} | "
                f"RPS: {stats['current_rps']} | "
                f"Total Req: {stats['total_requests']} | "
                f"Errors: {stats['total_errors']}"
            )

    async def start(self) -> None:
        """Запускает пул воркеров, асинхронный TCP-сервер и фоновый мониторинг метрик."""
        # 1. Запуск пула фоновых асинхронных воркеров для разбора очереди
        for _ in range(self._num_workers):
            worker_task = asyncio.create_task(self._worker_loop())
            self._background_tasks.add(worker_task)
            worker_task.add_done_callback(self._background_tasks.discard)

        # 2. Старт сетевого асинхронного TCP-сервера
        self._server = await asyncio.start_server(
            self._dispatch_client,
            self._settings.listen_host,
            self._settings.listen_port,
            limit=65536
        )

        addr = self._server.sockets[0].getsockname()
        logger.info(f"High-Performance Async Mini-Nginx running on http://{addr[0]}:{addr[1]}")

        # 3. Запуск фоновой задачи периодического вывода статистики
        metrics_task = asyncio.create_task(self._log_metrics_loop())
        self._background_tasks.add(metrics_task)
        metrics_task.add_done_callback(self._background_tasks.discard)

        async with self._server:
            await self._server.serve_forever()
