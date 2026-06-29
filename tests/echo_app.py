import asyncio
import sys

from proxy.core.config import HTTP_HEADER_DELIMITER
from proxy.core.logger import logger


async def handle_echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Асинхронный обработчик, который возвращает тело запроса обратно клиенту."""
    header_data = b""
    # 1. Чтение входящего потока до тех пор, пока не встретится конец заголовков HTTP
    while HTTP_HEADER_DELIMITER not in header_data:
        chunk = await reader.read(4096)
        if not chunk:
            break
        header_data += chunk

    if not header_data:
        writer.close()
        return

    # 2. Вытаскивание Content-Length из прочитанных заголовков, чтобы понять размер тела
    content_length = 0
    lines = header_data.decode('utf-8', errors='ignore').split('\r\n')
    for line in lines:
        if line.lower().startswith("content-length:"):
            try:
                content_length = int(line.split(":", 1)[1].strip())
            except ValueError as err:
                logger.warning(f"Malformed Content-Length header skipped: {err}")

    # 3. Формирование стандартного HTTP/1.1 ответа
    response_headers = (
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        f"Content-Length: {content_length}\r\n"
        "Connection: close\r\n\r\n"
    )
    writer.write(response_headers.encode('utf-8'))
    await writer.drain()

    # 4. Потоковый эхо-стриминг тела
    bytes_read = 0
    # Если часть тела запроса уже успела прочитаться вместе с заголовками в первом цикле
    if HTTP_HEADER_DELIMITER in header_data:
        body_start = header_data.split(HTTP_HEADER_DELIMITER, 1)[1]
        if body_start:
            writer.write(body_start)
            await writer.drain()
            bytes_read += len(body_start)

    # Дочитывание оставшихся 'кусков' тела из сокета и сразу пишем их обратно клиенту
    while bytes_read < content_length:
        to_read = min(65536, content_length - bytes_read)
        chunk = await reader.read(to_read)
        if not chunk:
            break
        writer.write(chunk)
        await writer.drain()
        bytes_read += len(chunk)

    # Корректное закрытие соединения
    writer.close()
    await writer.wait_closed()


async def main():
    # Порт передается аргументом командной строки (например, python echo_app.py 9001)
    # Если аргумента нет, по умолчанию берется порт 9001
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9001

    server = await asyncio.start_server(handle_echo, '127.0.0.1', port)
    print(f"[UPSTREAM] Echo server running on http://127.0.0.1:{port}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Upstream echo server stopped by user.")
