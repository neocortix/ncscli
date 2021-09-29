import http from 'k6/http';
import { sleep } from 'k6';

export let options = {
  stages: [
    { duration: '5s',  target: 1 },
    { duration: '5s',  target: 2 },
    { duration: '5s',  target: 3 },
    { duration: '5s',  target: 4 },
    { duration: '5s',  target: 5 },
    { duration: '5s',  target: 6 },
    { duration: '60s', target: 6 },
  ],
};

export default function() {
  http.get('https://loadtest-target.neocortix.com');
  sleep(1);
}
