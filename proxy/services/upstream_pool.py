import asyncio

from proxy.core.config import UpstreamConfig
from proxy.core.logger import logger


class UpstreamConnection:
    """Контейнер для хранения живого переиспользуемого TCP-соединения с апстримом."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, expires_at: float):
        self.reader = reader
        self.writer = writer
        self.expires_at = expires_at


class UpstreamPool:
    """'Умный' пул соединений с балансировщиком Round-Robin и контролем лимитов."""

    def __init__(self, upstreams: list[UpstreamConfig], max_conns_per_upstream: int, keepalive_timeout: float = 60.0):
        # Превращение конфиг-объекты в простые кортежи (host, port)
        self.endpoints = [(u.host, u.port) for u in upstreams]
        self.max_conns = max_conns_per_upstream
        self.keepalive_timeout = keepalive_timeout
        self.index = 0
        self.lock = asyncio.Lock()

        # Создание семафора ограничений для каждого апстрима отдельно!!!
        self.semaphores = {addr: asyncio.Semaphore(max_conns_per_upstream) for addr in self.endpoints}

        # Хранилище свободных keep-alive соединений: { (host, port): [UpstreamConnection, ...] }
        self.pools: dict[tuple[str, int], list[UpstreamConnection]] = {addr: [] for addr in self.endpoints}

    async def acquire(self) -> tuple[tuple[str, int], asyncio.StreamReader, asyncio.StreamWriter, asyncio.Semaphore]:
        """
        Выбирает апстрим по Round-Robin, резервирует слот через семафор
        строит или достает из пула готовое TCP-соединение.
        """
        # 1. Атомарно выбирается следующий хост по кругу (Round-Robin)
        async with self.lock:
            if not self.endpoints:
                raise RuntimeError("No upstreams configured in pool")
            addr = self.endpoints[self.index]
            self.index = (self.index + 1) % len(self.endpoints)

        sem = self.semaphores[addr]

        # 2. Ожидание свободного слота для этого апстрима (Backpressure / Сдерживание перегрузки)
        await sem.acquire()

        loop = asyncio.get_running_loop()
        now = loop.time()

        # 3. Попытка достать живое Keep-Alive соединение из пула
        # Гарантия изоляции: берем пул строго для выбранного хоста и порта
        specific_pool = self.pools.get(addr, [])
        while specific_pool:
            conn = specific_pool.pop()

            # Если сокет уже закрывается или закрыт системой — пропускаем его
            if conn.writer.is_closing():
                continue

            # Если время жизни соединения не истекло — используем повторно!
            if conn.expires_at > now:
                return addr, conn.reader, conn.writer, sem
            else:
                # Время вышло, закрываем протухший сокет
                conn.writer.close()
                try:
                    await conn.writer.wait_closed()
                except Exception as err:
                    logger.error(f"Error closing expired upstream connection during acquire to {addr}: {err}")

        # 4. Если в пуле не нашлось живого соединения — нужно открыть новое TCP-соединение
        # Устанавка limit=65536 байт для внутреннего буфера StreamReader, чтобы контролировать RAM
        reader, writer = await asyncio.open_connection(addr[0], addr[1], limit=65536)
        return addr, reader, writer, sem

    async def release(self, addr: tuple[str, int], reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                      sem: asyncio.Semaphore, keep_alive: bool):
        """
        Возвращает соединение обратно в пул для повторного использования
        Или окончательно закрывает его, если Keep-Alive не поддерживается.
        """
        try:
            if keep_alive and not writer.is_closing():
                loop = asyncio.get_running_loop()
                # Расчет метки времени, до которой сокет будет считаться валидным
                expires_at = loop.time() + self.keepalive_timeout
                conn = UpstreamConnection(reader, writer, expires_at)
                self.pools[addr].append(conn)
            else:
                # Если апстрим или клиент попросили close — гасим соединение ОБЯЗАТЕЛЬНО!!!
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception as err:
                    logger.error(f"Error wait_closed during forced upstream release to {addr}: {err}")
        finally:
            # В ЛЮБОМ СЛУЧАЕ освобождаем семафор, чтобы дать дорогу другим запросам
            sem.release()
