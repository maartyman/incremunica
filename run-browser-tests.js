const crypto = require('crypto');
const http = require('http');
const { spawn } = require('node:child_process');
const WebSocket = require('ws');

const sourceData = {};

function hashString(str) {
  const hash = crypto.createHash('sha256').update(str).digest('hex');
  return hash;
}

const server = http.createServer((req, res) => {
  if (req.url.startsWith('/reset')) {
    let body = '';
    req.on('data', (data) => {
      body += data;
    });
    req.on('end', () => {
      sourceData[req.url.slice(6)] = body;
      res.statusCode = 200;

      res.setHeader('access-control-allow-origin', '*');
      res.setHeader('access-control-allow-credentials', 'true');
      res.setHeader('access-control-expose-headers', 'Accept-Patch,Accept-Post,Accept-Put,Allow,Content-Range,ETag,Last-Modified,Link,Location,Updates-Via,WAC-Allow,Www-Authenticate');
      res.setHeader('content-type', 'text/turtle');
      res.setHeader('connection', 'close');
      res.setHeader('transfer-encoding', 'chunked');

      res.end('');
    });
  } else if (req.url.endsWith('/.well-known/solid')) {
    res.statusCode = 200;

    res.setHeader('vary', 'Accept,Authorization,Origin');
    res.setHeader('x-powered-by', 'Community Solid Server');
    res.setHeader('access-control-allow-origin', '*');
    res.setHeader('access-control-allow-credentials', 'true');
    res.setHeader('access-control-expose-headers', 'Accept-Patch,Accept-Post,Accept-Put,Allow,Content-Range,ETag,Last-Modified,Link,Location,Updates-Via,WAC-Allow,Www-Authenticate');
    res.setHeader('content-type', 'text/turtle');
    res.setHeader('etag', '1700215601000-text/turtle');
    res.setHeader('date', 'Fri, 17 Nov 2023 17:15:00 GMT');
    res.setHeader('connection', 'close');
    res.setHeader('transfer-encoding', 'chunked');
    res.setHeader('cache-control', 'no-cache');

    if (req.method === 'HEAD') {
      res.end();
      return;
    }

    res.end(`
@prefix notify: <http://www.w3.org/ns/solid/notifications#> .
@prefix pim: <http://www.w3.org/ns/pim/space#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

<http://localhost:3000/> rdf:type pim:Storage ;
notify:subscription <http://localhost:3000/.notifications/WebhookChannel2023/> ,
<http://localhost:3000/.notifications/WebSocketChannel2023/> .

<http://localhost:3000/.notifications/WebhookChannel2023/> notify:channelType notify:WebhookChannel2023 ;
notify:feature notify:accept ,
notify:endAt ,
notify:rate ,
notify:startAt ,
notify:state .

<http://localhost:3000/.notifications/WebSocketChannel2023/> notify:channelType notify:WebSocketChannel2023 ;
notify:feature notify:accept ,
notify:endAt ,
notify:rate ,
notify:startAt ,
notify:state .`);
  } else if (req.url.endsWith('/.notifications/WebSocketChannel2023/')) {
    if (req.method === 'OPTIONS') {
      res.statusCode = 200;

      res.setHeader('access-control-allow-origin', '*');
      res.setHeader('access-control-allow-methods', 'OPTIONS, HEAD, POST');
      res.setHeader('access-control-allow-headers', 'Accept-Patch,Accept-Post,Accept-Put,Allow,Content-Range,ETag,Last-Modified,Link,Location,Updates-Via,WAC-Allow,Www-Authenticate,content-type');
      res.setHeader('access-control-allow-credentials', 'true');

      res.end();
    }
    if (req.method === 'POST') {
      res.statusCode = 200;

      res.setHeader('vary', 'Accept,Authorization,Origin');
      res.setHeader('x-powered-by', 'Community Solid Server');
      res.setHeader('access-control-allow-origin', '*');
      res.setHeader('access-control-allow-credentials', 'true');
      res.setHeader('access-control-expose-headers', 'Accept-Patch,Accept-Post,Accept-Put,Allow,Content-Range,ETag,Last-Modified,Link,Location,Updates-Via,WAC-Allow,Www-Authenticate');
      res.setHeader('content-type', 'text/turtle');
      res.setHeader('etag', '1700215601000-text/turtle');
      res.setHeader('date', 'Fri, 17 Nov 2023 17:15:00 GMT');
      res.setHeader('connection', 'close');
      res.setHeader('transfer-encoding', 'chunked');

      res.end(`
<> <http://www.w3.org/ns/solid/notifications#topic> <http://localhost:3000/>;
    <http://www.w3.org/ns/solid/notifications#receiveFrom> <ws://localhost:4015>;
    <http://www.w3.org/ns/solid/notifications#endAt> "2023-12-02T11:36:15.651Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
    a <http://www.w3.org/ns/solid/notifications#WebSocketChannel2023>.`);
    }
  } else {
    res.statusCode = 200;

    const responseData = sourceData[req.url] || JSON.stringify(sourceData);

    res.setHeader('vary', 'Accept,Authorization,Origin');
    res.setHeader('x-powered-by', 'Community Solid Server');
    res.setHeader('access-control-allow-origin', '*');
    res.setHeader('access-control-allow-credentials', 'true');
    res.setHeader('access-control-expose-headers', 'Accept-Patch,Accept-Post,Accept-Put,Allow,Content-Range,ETag,Last-Modified,Link,Location,Updates-Via,WAC-Allow,Www-Authenticate');
    res.setHeader('wac-allow', 'user="read",public="read"');
    res.setHeader('allow', 'OPTIONS, HEAD, GET, PATCH, POST');
    res.setHeader('accept-patch', 'text/n3, application/sparql-update');
    res.setHeader('accept-post', '*/*');
    res.setHeader('content-type', 'text/turtle');
    res.setHeader('link', '<http://www.w3.org/ns/pim/space#Storage>; rel="type", <http://www.w3.org/ns/ldp#Container>; rel="type", <http://www.w3.org/ns/ldp#BasicContainer>; rel="type", <http://www.w3.org/ns/ldp#Resource>; rel="type", <http://localhost:3000/pod1/.meta>; rel="describedby", <http://localhost:3000/pod1/.acl>; rel="acl", <http://localhost:3000/.well-known/solid>; rel="http://www.w3.org/ns/solid/terms#storageDescription"');
    res.setHeader('last-modified', 'Fri, 17 Nov 2023 10:06:41 GMT');
    res.setHeader('etag', hashString(responseData));
    res.setHeader('date', 'Fri, 17 Nov 2023 17:15:00 GMT');
    res.setHeader('connection', 'close');
    res.setHeader('cache-control', 'no-cache');

    res.end(responseData);
  }
});

server.listen(3000);
// eslint-disable-next-line no-console
console.log('Server running at http://localhost:3000/');

const message = {
  '@context': [
    'https://www.w3.org/ns/activitystreams',
    'https://www.w3.org/ns/solid/notification/v1',
  ],
  type: 'Update',
  id: 'urn:1700310987535:http://localhost:3000/',
  object: 'http://localhost:3000/',
  published: '2023-11-18T12:36:27.535Z',
  state: '1700311389105-text/turtle',
};

const websocket = new WebSocket.WebSocketServer({ port: 4015 });
websocket.on('connection', (ws) => {
  setTimeout(() => {
    ws.send(JSON.stringify(message));
  }, 500);
});
// eslint-disable-next-line no-console
console.log('WebSocket server running at ws://localhost:4015/');

function endServers() {
  websocket.close();
  server.close();
  // eslint-disable-next-line no-console
  console.log('Servers stopped');
}

// eslint-disable-next-line no-console
console.log('Starting Karma tests:');
const lsProcess = spawn('karma', [ 'start', 'karma.config.js', '--single-run' ]);
lsProcess.stdout.on('data', (data) => {
  process.stdout.write(data);
});

lsProcess.stderr.on('data', (data) => {
  endServers();
  throw new Error(data);
});

lsProcess.on('close', async(code) => {
  endServers();
  if (code !== 0) {
    throw new Error(`child process exited with code ${code}`);
  }
});
