import asyncio
from proxy.logger import logger
from proxy.metrics import metrics
from proxy.utils.http import HTTPParser
from proxy.timeouts import TimeoutPolicy
from proxy.upstream_pool import UpstreamPool
from proxy.config import settings


class ClientConnectionHandler:
    """Ядро прокси: обрабатывает сессии, стриминг и защищает от переполнения буферов."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, upstream_pool: UpstreamPool):
        self.client_reader = reader
        self.client_writer = writer
        self.pool = upstream_pool
        self.buffer_size = 65536  # Оптимальный чанк 64KB для потоковой передачи
        self.upstream_should_close = False

    async def handle(self) -> None:
        """Точка входа обработки клиента. Поддерживает HTTP/1.1 Keep-Alive конвейер."""
        metrics.conn_start()
        keep_alive = True

        while keep_alive and not self.client_writer.is_closing():
            try:
                # Каждая итерация — это обработка одного HTTP-запроса в рамках сессии!!! Запомнить!
                keep_alive = await asyncio.wait_for(
                    self._process_single_request(),
                    timeout=settings.timeouts.total
                )
            except asyncio.TimeoutError:
                logger.debug("Session or connection stage timed out. Closing client slot.")
                break
            except (ConnectionResetError, BrokenPipeError):
                logger.debug("Client abruptly disconnected.")
                break
            except Exception as e:
                logger.error(f"Error pipelines execution: {e}", exc_info=True)
                metrics.inc_error()
                await self._safe_send_error(502, b"Bad Gateway")
                break

        metrics.conn_end()
        await self._close_stream(self.client_writer)

    async def _process_single_request(self) -> bool:
        """Разбирает заголовки, бронирует апстрим и запускает двунаправленный стрим."""
        header_data = b""
        while b"\r\n\r\n" not in header_data:
            chunk = await TimeoutPolicy.run_with_timeout(
                self.client_reader.read(4096),
                settings.timeouts.read, "reading headers from client"
            )
            if not chunk:
                return False  # Клиент ушел в EOF
            header_data += chunk

        headers_part, body_start = header_data.split(b"\r\n\r\n", 1)
        method, path, version, headers = HTTPParser.parse_headers(headers_part)

        client_keep_alive = HTTPParser.should_keep_alive(version, headers)

        # Получение готового TCP-канала и семафора из UpstreamPool
        addr, up_reader, up_writer, sem = await self.pool.acquire()

        upstream_keep_alive = False
        try:
            # Проброс стартовой строки и заголовка апстриму
            up_writer.write(headers_part + b"\r\n\r\n")
            if body_start:
                up_writer.write(body_start)
            await TimeoutPolicy.run_with_timeout(up_writer.drain(), settings.timeouts.write,
                                                 "pushing headers to upstream")

            # Конкурентный запуск асинхронных стримов чтения и записи (Client <-> Upstream)
            c2u_task = asyncio.create_task(self._stream_client_to_upstream(self.client_reader, up_writer, headers))
            u2c_task = asyncio.create_task(self._stream_upstream_to_client(up_reader, self.client_writer))

            # Ожидание завершения прокачки данных в обоих направлениях
            await asyncio.gather(c2u_task, u2c_task)

            # Если апстрим или клиент явно просили закрыть, или сокет подыхает — никакого keep-alive!
            if client_keep_alive and not self.upstream_should_close and not up_writer.is_closing():
                upstream_keep_alive = True

        except Exception as e:
            logger.error(f"Streaming failure via upstream {addr}: {e}")
            raise e
        finally:
            # Возврат сокета в пул или корректное его гащение
            await self.pool.release(addr, up_reader, up_writer, sem, keep_alive=upstream_keep_alive)

        return client_keep_alive

    async def _stream_client_to_upstream(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                                         headers: dict) -> None:
        """Стримит тело запроса (например, POST-данные) от клиента в апстрим."""
        content_length = int(headers.get('content-length', 0))
        is_chunked = headers.get('transfer-encoding', '').lower() == 'chunked'

        if content_length > 0:
            bytes_sent = 0
            while bytes_sent < content_length:
                to_read = min(self.buffer_size, content_length - bytes_sent)
                data = await TimeoutPolicy.run_with_timeout(reader.read(to_read), settings.timeouts.read,
                                                            "reading client body")
                if not data:
                    break
                writer.write(data)
                # БОРЬБА С BACKPRESSURE: ожидание очистки буфера сокета, если он переполнен
                await TimeoutPolicy.run_with_timeout(writer.drain(), settings.timeouts.write,
                                                     "draining upstream buffer")
                bytes_sent += len(data)
        elif is_chunked:
            while True:
                chunk = await TimeoutPolicy.run_with_timeout(reader.read(self.buffer_size), settings.timeouts.read,
                                                             "reading chunked client body")
                if not chunk:
                    break
                writer.write(chunk)
                await TimeoutPolicy.run_with_timeout(writer.drain(), settings.timeouts.write,
                                                     "draining chunked upstream buffer")
                if chunk.endswith(b"0\r\n\r\n"):
                    break

    async def _stream_upstream_to_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Стримит ответ апстрима обратно клиенту с разбором HTTP-структуры ответа."""
        header_data = b""
        while b"\r\n\r\n" not in header_data:
            chunk = await TimeoutPolicy.run_with_timeout(reader.read(4096), settings.timeouts.read,
                                                         "reading upstream headers")
            if not chunk:
                return
            header_data += chunk

        headers_part, body_start = header_data.split(b"\r\n\r\n", 1)

        # Отправка клиенту заголовки ответа
        writer.write(headers_part + b"\r\n\r\n")
        if body_start:
            writer.write(body_start)
        await TimeoutPolicy.run_with_timeout(writer.drain(), settings.timeouts.write,
                                             "draining client response headers buffer")

        # Парсинг заголовков ответа для корректной потоковой выкачки тела
        lines = headers_part.decode('utf-8', errors='ignore').split('\r\n')
        up_headers = {line.split(':', 1)[0].strip().lower(): line.split(':', 1)[1].strip()
                      for line in lines[1:] if ':' in line}

        if up_headers.get('connection', '').lower() == 'close':
            self.upstream_should_close = True

        content_length = int(up_headers.get('content-length', -1))
        is_chunked = up_headers.get('transfer-encoding', '').lower() == 'chunked'

        if content_length >= 0:
            bytes_sent = len(body_start)
            while bytes_sent < content_length:
                to_read = min(self.buffer_size, content_length - bytes_sent)
                data = await TimeoutPolicy.run_with_timeout(reader.read(to_read), settings.timeouts.read,
                                                            "reading upstream body")
                if not data:
                    break
                writer.write(data)
                await TimeoutPolicy.run_with_timeout(writer.drain(), settings.timeouts.write,
                                                     "draining client body response buffer")
                bytes_sent += len(data)
        else:
            while True:
                data = await TimeoutPolicy.run_with_timeout(reader.read(self.buffer_size), settings.timeouts.read,
                                                            "reading streaming upstream body")
                if not data:
                    break
                writer.write(data)
                await TimeoutPolicy.run_with_timeout(writer.drain(), settings.timeouts.write,
                                                     "draining client dynamic response buffer")
                if is_chunked and data.endswith(b"0\r\n\r\n"):
                    break

    async def _safe_send_error(self, status: int, message: bytes) -> None:
        try:
            res = f"HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: {len(message)}\r\n\r\n".encode() + message
            self.client_writer.write(res)
            await self.client_writer.drain()
        except:
            pass

    async def _close_stream(self, writer: asyncio.StreamWriter) -> None:
        try:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except:
            pass
