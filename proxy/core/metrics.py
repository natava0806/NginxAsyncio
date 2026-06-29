import time


class MetricsCollector:
    """Атомарный сборщик метрик для мониторинга состояния Mini-Nginx."""

    def __init__(self):
        self.active_connections: int = 0
        self.total_requests: int = 0
        self.total_errors: int = 0
        self.start_time: float = time.time()

    def conn_start(self) -> None:
        """Регистрирует новое входящее клиентское соединение."""
        self.active_connections += 1
        self.total_requests += 1

    def conn_end(self) -> None:
        """Регистрирует закрытие клиентского соединения."""
        # Защита от ухода счетчика в отрицательные значения при сбоях
        self.active_connections = max(0, self.active_connections - 1)

    def inc_error(self) -> None:
        """Инкрементирует счетчик сетевых или внутренних ошибок прокси."""
        self.total_errors += 1

    def get_stats(self) -> dict:
        """Вычисляет текущие метрики эффективности (включая RPS) и возвращает словарь."""
        uptime = time.time() - self.start_time
        # Защита от деления на ноль при мгновенном запросе статистики после старта
        rps = self.total_requests / uptime if uptime > 0 else 0.0

        return {
            "uptime_seconds": round(uptime, 2),
            "active_connections": self.active_connections,
            "total_requests": self.total_requests,
            "total_errors": self.total_errors,
            "current_rps": round(rps, 2)
        }


# Экспорт единственного глобального экземпляра (Singleton) для всей программы
metrics = MetricsCollector()
