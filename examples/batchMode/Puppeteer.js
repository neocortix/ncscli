#!/usr/bin/env node

const puppeteer = require('puppeteer');
const fs = require('fs');

(async () => {
  const url = 'https://www.google.com';
  const image = 'google.png';

  if (fs.existsSync(image))
    fs.unlinkSync(image);

  const browser = await puppeteer.launch({
    headless: true,
    defaultViewport: {
      width:  1920,
      height: 1080,
    },
    args: ['--no-sandbox']
  });
  const page = await browser.newPage();
  await page.goto(url);
  await page.screenshot({path: image});

  await browser.close();
})();
