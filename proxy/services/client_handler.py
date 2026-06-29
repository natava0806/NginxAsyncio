import asyncio

from proxy.core.config import HTTP_HEADER_DELIMITER, settings
from proxy.core.logger import logger
from proxy.core.metrics import metrics
from proxy.core.timeouts import TimeoutPolicy
from proxy.services.http_parser import HTTPParser
from proxy.services.upstream_pool import UpstreamPool


HTTP_HEADER_DELIMITER_WITH_0 = b"0\r\n\r\n"


class ClientConnectionHandler:
    """Ядро прокси: обрабатывает сессии, стриминг и защищает от переполнения буферов."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, upstream_pool: UpstreamPool):
        self._client_reader = reader
        self._client_writer = writer
        self._pool = upstream_pool
        self._buffer_size = 65536  # Оптимальный чанк 64KB для потоковой передачи
        self._upstream_should_close = False

    async def handle(self) -> None:
        """Точка входа обработки клиента. Поддерживает HTTP/1.1 Keep-Alive конвейер."""
        metrics.conn_start()
        keep_alive = True

        while keep_alive and not self._client_writer.is_closing():
            try:
                # Каждая итерация — это обработка одного HTTP-запроса в рамках сессии!!! Запомнить!
                keep_alive = await asyncio.wait_for(
                    self._process_single_request,
                    timeout=settings.timeouts.total
                )
            except TimeoutError:
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
        await self._close_stream(self._client_writer)

    @property
    async def _process_single_request(self) -> bool:
        """Разбирает заголовки, бронирует апстрим и запускает двунаправленный стрим."""
        header_data = b""
        while HTTP_HEADER_DELIMITER not in header_data:
            chunk = await TimeoutPolicy.run_with_timeout(
                self._client_reader.read(4096),
                settings.timeouts.read, "reading headers from client"
            )
            if not chunk:
                return False  # Клиент ушел в EOF
            header_data += chunk

        headers_part, body_start = header_data.split(HTTP_HEADER_DELIMITER, 1)
        method, path, version, headers = HTTPParser.parse_headers(headers_part)
        client_keep_alive = HTTPParser.should_keep_alive(version, headers)
        # Получение готового TCP-канала и семафора из UpstreamPool
        addr, up_reader, up_writer, sem = await self._pool.acquire()
        upstream_keep_alive = False

        try:
            # Проброс стартовой строки и заголовка апстриму
            up_writer.write(headers_part + HTTP_HEADER_DELIMITER)
            if body_start:
                up_writer.write(body_start)
            await TimeoutPolicy.run_with_timeout(up_writer.drain(), settings.timeouts.write,
                                                 "pushing headers to upstream")

            # Конкурентный запуск асинхронных стримов чтения и записи (Client <-> Upstream)
            c2u_task = asyncio.create_task(self._stream_client_to_upstream(self._client_reader, up_writer, headers))
            u2c_task = asyncio.create_task(self._stream_upstream_to_client(up_reader, self._client_writer))
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
            await self._pool.release(addr, up_reader, up_writer, sem, keep_alive=upstream_keep_alive)

        return client_keep_alive

    async def _stream_client_to_upstream(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                                         headers: dict) -> None:
        """Стримит тело запроса (например, POST-данные) от клиента в апстрим."""
        content_length = int(headers.get('content-length', 0))
        is_chunked = headers.get('transfer-encoding', '').lower() == 'chunked'

        if content_length > 0:
            bytes_sent = 0
            while bytes_sent < content_length:
                to_read = min(self._buffer_size, content_length - bytes_sent)
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
                chunk = await TimeoutPolicy.run_with_timeout(reader.read(self._buffer_size), settings.timeouts.read,
                                                             "reading chunked client body")
                if not chunk:
                    break
                writer.write(chunk)
                await TimeoutPolicy.run_with_timeout(writer.drain(), settings.timeouts.write,
                                                     "draining chunked upstream buffer")
                if chunk.endswith(HTTP_HEADER_DELIMITER_WITH_0):
                    break

    async def _stream_upstream_to_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Стримит ответ апстрима обратно клиенту с разбором HTTP-структуры ответа."""
        header_data = b""
        while HTTP_HEADER_DELIMITER not in header_data:
            chunk = await TimeoutPolicy.run_with_timeout(
                reader.read(4096), settings.timeouts.read, "reading upstream headers"
            )
            if not chunk:
                return
            header_data += chunk

        headers_part, body_start = header_data.split(HTTP_HEADER_DELIMITER, 1)

        # Отправка клиенту заголовки ответа
        writer.write(headers_part + HTTP_HEADER_DELIMITER)
        if body_start:
            writer.write(body_start)
        await TimeoutPolicy.run_with_timeout(writer.drain(), settings.timeouts.write,
                                             "draining client response headers buffer")

        # Парсинг заголовков ответа для корректной потоковой выкачки тела
        up_headers = self._parse_upstream_headers(headers_part)
        if up_headers.get('connection', '').lower() == 'close':
            self.upstream_should_close = True

        content_length = int(up_headers.get('content-length', -1))
        is_chunked = up_headers.get('transfer-encoding', '').lower() == 'chunked'

        if content_length >= 0:
            await self._stream_fixed_content(reader, body_start, content_length)
        else:
            if body_start:
                writer.write(body_start)
                await TimeoutPolicy.run_with_timeout(writer.drain(), settings.timeouts.write, "draining initial chunk")
            await self._stream_dynamic_content(reader, is_chunked)

    async def _safe_send_error(self, status: int, message: bytes) -> None:
        try:
            res = f"HTTP/1.1 {status}\r\nConnection: close\r\nContent-Length: {len(message)}\r\n\r\n".encode() + message
            self._client_writer.write(res)
            await self._client_writer.drain()
        except Exception as err:
            logger.error(f"Failed to send safe error response to client: {err}")

    async def _close_stream(self, writer: asyncio.StreamWriter) -> None:
        try:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except Exception as err:
            logger.error(f"Error occurred during graceful stream closure: {err}")

    @staticmethod
    def _parse_upstream_headers(headers_part: bytes) -> dict:
        """[ПОДСУШЕНО] Вспомогательный метод для очистки основного кода от парсинга строк."""
        lines = headers_part.decode('utf-8', errors='ignore').split('\r\n')
        up_headers = {}
        for line in lines[1:]:
            if ':' in line:
                k, v = line.split(':', 1)
                up_headers[k.strip().lower()] = v.strip()
        return up_headers

    async def _stream_fixed_content(self, reader: asyncio.StreamReader, body_start: bytes, content_length: int) -> None:
        """[ПОДСУШЕНО] Выкачка тела ответа фиксированной длины Content-Length."""
        bytes_sent = len(body_start)
        while bytes_sent < content_length:
            to_read = min(self._buffer_size, content_length - bytes_sent)
            data = await TimeoutPolicy.run_with_timeout(
                reader.read(to_read), settings.timeouts.read, "reading upstream body"
            )
            if not data:
                break
            self._client_writer.write(data)
            await TimeoutPolicy.run_with_timeout(
                self._client_writer.drain(), settings.timeouts.write, "draining client body response buffer"
            )
            bytes_sent += len(data)

    async def _stream_dynamic_content(self, reader: asyncio.StreamReader, is_chunked: bool) -> None:
        """[ПОДСУШЕНО] Выкачка динамического ответа (Chunked Transfer Encoding или до EOF)."""
        while True:
            data = await TimeoutPolicy.run_with_timeout(
                reader.read(self._buffer_size), settings.timeouts.read, "reading streaming upstream body"
            )
            if not data:
                break
            self._client_writer.write(data)
            await TimeoutPolicy.run_with_timeout(
                self._client_writer.drain(), settings.timeouts.write, "draining client dynamic response buffer"
            )
            if is_chunked and data.endswith(HTTP_HEADER_DELIMITER_WITH_0):
                break
