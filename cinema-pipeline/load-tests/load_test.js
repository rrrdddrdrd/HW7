import http from 'k6/http';
import { check, sleep } from 'k6';

const PRODUCER_URL = __ENV.PRODUCER_URL || 'http://localhost:8000';

const EVENT_TYPES = ['VIEW_STARTED', 'VIEW_FINISHED', 'VIEW_PAUSED', 'VIEW_RESUMED', 'LIKED'];
const DEVICE_TYPES = ['DESKTOP', 'MOBILE', 'TV', 'TABLET'];

export const options = {
  vus: 10,
  duration: '30s',
  thresholds: {
    http_req_duration: ['p(95)<500'],
    http_req_failed: ['rate<0.01'],
    http_reqs: ['rate>5'],
  },
};

function randomUUID() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

export default function () {
  const userNum = Math.floor(Math.random() * 100) + 1;
  const movieNum = Math.floor(Math.random() * 30) + 1;

  const payload = JSON.stringify({
    user_id: `user_${String(userNum).padStart(4, '0')}`,
    movie_id: `movie_${String(movieNum).padStart(3, '0')}`,
    event_type: randomItem(EVENT_TYPES),
    device_type: randomItem(DEVICE_TYPES),
    session_id: randomUUID(),
    progress_seconds: Math.floor(Math.random() * 7200),
  });

  const res = http.post(`${PRODUCER_URL}/events`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status 202': (r) => r.status === 202,
    'has event_id': (r) => {
      try {
        return JSON.parse(r.body).event_id !== undefined;
      } catch (_) {
        return false;
      }
    },
  });

  sleep(0.1);
}

export function handleSummary(data) {
  return {
    '/scripts/results/summary.json': JSON.stringify(data, null, 2),
    stdout: JSON.stringify(
      {
        vus: data.metrics.vus ? data.metrics.vus.values.value : 0,
        requests: data.metrics.http_reqs ? data.metrics.http_reqs.values.count : 0,
        failed_rate: data.metrics.http_req_failed
          ? data.metrics.http_req_failed.values.rate
          : 0,
        p95_ms: data.metrics.http_req_duration
          ? data.metrics.http_req_duration.values['p(95)']
          : 0,
      },
      null,
      2
    ),
  };
}
