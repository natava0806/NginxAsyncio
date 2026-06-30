import asyncio


class TimeoutPolicy:
    """Политика управления временем ожидания сетевых операций."""

    @staticmethod
    async def run_with_timeout(coro, timeout_sec: float, context_err: str):
        """
        Оборачивает асинхронную корутину в ограничение по времени (wait_for).
        Если операция не успевает выполниться, выбрасывает подробный TimeoutError.
        """
        try:
            return await asyncio.wait_for(coro, timeout=timeout_sec)
        except TimeoutError:
            raise TimeoutError(f"Operation timed out: {context_err} ({timeout_sec}s)") from None
