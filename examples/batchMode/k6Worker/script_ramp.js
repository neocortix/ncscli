import http from 'k6/http';
import { sleep } from 'k6';

export let options = {
  stages: [
    { duration: '3s',  target: 1 },
    { duration: '57s', target: 1 },
    { duration: '3s',  target: 2 },
    { duration: '57s', target: 2 },
    { duration: '3s',  target: 3 },
    { duration: '57s', target: 3 },
    { duration: '3s',  target: 4 },
    { duration: '57s', target: 4 },
    { duration: '3s',  target: 5 },
    { duration: '57s', target: 5 },
    { duration: '3s',  target: 6 },
    { duration: '57s', target: 6 },
    { duration: '137s',target: 6 },
    { duration: '3s',  target: 0 }
  ],
};

export default function() {
  http.get('https://loadtest-target.neocortix.com');
  sleep(1);
}
