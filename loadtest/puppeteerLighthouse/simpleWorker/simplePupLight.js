const fs = require('fs');
const puppeteer = require('puppeteer');
const lighthouse = require('lighthouse');
//const reportGenerator = require('lighthouse/lighthouse-core/report/report-generator');
const {URL} = require('url');

/**
 * Decode base64 image
 *.e.g. data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAFo...blahblahblah
 */
 function decodeBase64Image(dataString) {
  var matches = dataString.match(/^data:([A-Za-z-+\/]+);base64,(.+)$/),
    response = {};

  if (matches.length !== 3) {
    return new Error('Invalid input string');
  }

  response.type = matches[1];
  response.data = Buffer.from(matches[2], 'base64');

  return response;
}


// process (positional) cmd-line args frameNum and url, if present
let frameNum = 0;
if (process.argv.length > 2) {
  frameNum = process.argv[2];
  //console.log('frameNum:', frameNum);
}
let url = 'https://loadtest-target.neocortix.com';
if (process.argv.length > 3) {
  url =  process.argv[3];
  //console.log('url:', url);
}

(async() => {
  // Use Puppeteer to launch headless Chrome using a high-def viewport.
  const browser = await puppeteer.launch({
    headless: true,
    args: ['--no-sandbox'],  // may be dangerous, but required when running as root
    defaultViewport: {
      width:  1920,
      height: 1080,
    }
  });

  // ask Lighthouse to open the URL.
  const result = await lighthouse(url, {
    port: (new URL(browser.wsEndpoint())).port,
    output: 'html',
    logLevel: 'error',
  });
  const outDirPath = 'puppeteerOut';  // important, so the output files can be downloaded
  // write report files
  fs.writeFileSync(outDirPath+'/lighthouse.report.html', result.report );
  fs.writeFileSync(outDirPath+'/lighthouse.report.json', JSON.stringify( result.lhr, null, 2 ));
  //console.log(`Lighthouse scores: ${Object.values(lhr.categories).map(c => c.score).join(', ')}`);

  // extract and save the final screenshot
  //const screenie = result.lhr.audits["full-page-screenshot"].details.screenshot.data  // full size
  const screenie = result.lhr.audits["final-screenshot"].details.data  // shrunk
  if( screenie ) {
    decoded = decodeBase64Image( screenie );
    if( decoded.type.includes("jpeg") ) {
      fs.writeFileSync(outDirPath+'/lighthouse.screenshot.jpeg', decoded.data);
    }
    else if ( decoded.type.includes("png") ) {
      fs.writeFileSync(outDirPath+'/lighthouse.screenshot.png', decoded.data);
    }
    else {
      console.log('could not decode final screenshot; image type was ', decoded.type)
    }
  }

  await browser.close();
})();

