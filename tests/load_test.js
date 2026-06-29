import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    stages: [
        { duration: '5s', target: 50 },
        { duration: '15s', target: 300 }, // Твой мощный разгон на 300 VU
        { duration: '15s', target: 300 },
        { duration: '5s', target: 0 },
    ],
    thresholds: {
        http_req_failed: ['rate<0.05'],
        http_req_duration: ['p(95)<500'],
    },
};

export default function () {
    // 1. Тест POST-стриминга через прокси по localhost
    const resPost = http.post('http://localhost:8080/echo', 'hello from k6');
    check(resPost, { 'POST status is 200': (r) => r.status === 200 });

    sleep(0.1);

    // 2. Тест обычного GET через прокси по localhost
    const resGet = http.get('http://localhost:8080/');
    check(resGet, { 'GET status is 200': (r) => r.status === 200 });

    sleep(0.1);
}
