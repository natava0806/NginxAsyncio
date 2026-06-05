import http from 'k6/http';
import { check, sleep } from 'k6';

// Настройки нагрузки
export const options = {
    stages: [
        { duration: '10s', target: 50 },  // Разгон: плавное поднятие нагрузки до 50 пользователей
        { duration: '20s', target: 50 },  // Плато: нужно держать 50 одновременных коннектов
        { duration: '5s', target: 0 },    // Спад: плавное закрытие соединения
    ],
    thresholds: {
        http_req_failed: ['rate<0.01'],   // Критерий успешности: ошибок должно быть меньше 1%
        http_req_duration: ['p(95)<500'], // 95% запросов должны укладываться в 500мс
    },
};

export default function () {
    const url = 'http://127.0.0';
    const payload = JSON.stringify({ message: 'hello world from k6' });
    const params = {
        headers: { 'Content-Type': 'application/json' },
    };

    // 1. Тест POST-стриминга через прокси
    const resPost = http.post('http://localhost:8080/echo', 'hello from k6');
    check(resPost, { 'POST status is 200': (r) => r.status === 200 });

    sleep(0.1); // Небольшая пауза между запросами пользователя

    // 2. Тест обычного GET через прокси
    const resGet = http.get('http://localhost:8080/');
    check(resGet, { 'GET status is 200': (r) => r.status === 200 });

    sleep(0.1);
}
