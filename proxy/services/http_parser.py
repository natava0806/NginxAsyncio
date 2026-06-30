class HTTPParser:
    """Низкоуровневый парсер HTTP/1.1 текстовых протоколов."""

    @staticmethod
    def parse_headers(raw_headers: bytes) -> tuple[str, str, str, dict[str, str]]:
        """
        Принимает байтовую строку заголовков (до \r\n\r\n).
        Возвращает кортеж: (метод, путь, версия, словарь_заголовков).
        """
        # Декодировка байтов в строку, игнорируя битые символы
        lines = raw_headers.decode('utf-8', errors='ignore').split('\r\n')
        request_line = lines[0]

        # Разборка стартовой строки (например: "GET /index.html HTTP/1.1")
        parts = request_line.split(' ')
        if len(parts) < 3:
            raise ValueError("Malformed HTTP request line")

        method, path, version = parts[0], parts[1], parts[2]

        # Заполнение словаря заголовков
        headers = {}
        for line in lines[1:]:
            if not line or ':' not in line:
                continue
            k, v = line.split(':', 1)
            # Приведение ключей к нижнему регистру (HTTP/1.1 стандарт нечувствителен к регистру)
            headers[k.strip().lower()] = v.strip()

        return method, path, version, headers

    @staticmethod
    def should_keep_alive(version: str, headers: dict[str, str]) -> bool:
        """Определяет, требует ли соединение постоянного удержания (Keep-Alive)."""
        conn = headers.get('connection', '').lower()
        if version == 'HTTP/1.1':
            return conn != 'close'
        return conn == 'keep-alive'
