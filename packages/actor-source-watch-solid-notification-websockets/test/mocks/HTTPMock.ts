import { PassThrough } from 'readable-stream';

export function createFailedRequest(url: string) {
  const headersObj: any = {};
  headersObj.vary = 'Accept,Authorization,Origin';
  headersObj['x-powered-by'] = 'Community Solid Server';
  headersObj['access-control-allow-origin'] = '*';
  headersObj['access-control-allow-credentials'] = 'true';
  headersObj['access-control-expose-headers'] = `Accept-Patch,Accept-Post,Accept-Put,Allow,Content-Range,ETag,Last-Modified,Link,Location,Updates-Via,WAC-Allow,Www-Authenticate`;
  headersObj['content-type'] = 'application/json';
  headersObj.date = 'Fri, 17 Nov 2023 17:15:00 GMT';
  headersObj.connection = 'close';
  headersObj['transfer-encoding'] = 'chunked';
  headersObj.allow = 'PATCH, PUT';
  headersObj['accept-patch'] = 'text/n3, application/sparql-update';
  headersObj['accept-put'] = '*/*';

  const headers = new Headers();

  for (const key in headersObj) {
    headers.set(key, headersObj[key]);
  }

  const body = `{"name":"NotFoundHttpError","message":"","statusCode":404,"errorCode":"H404","details":{}}`;

  return {
    body: new PassThrough({
      read() {
        this.push(body);
        this.push(null);
      },
    }),
    headers,
    status: 404,
    statusText: 'Not Found',
    ok: false,
    url,
    clone: jest.fn(),
    error: jest.fn(),
    json: () => ({}),
    redirect: jest.fn(),
    text: () => body,
  };
}

export function createEmptyResourceRequest(url: string) {
  const headersObj: any = {};
  headersObj.vary = 'Accept,Authorization,Origin';
  headersObj['x-powered-by'] = 'Community Solid Server';
  headersObj['access-control-allow-origin'] = '*';
  headersObj['access-control-allow-credentials'] = 'true';
  headersObj['access-control-expose-headers'] = `Accept-Patch,Accept-Post,Accept-Put,Allow,Content-Range,ETag,Last-Modified,Link,Location,Updates-Via,WAC-Allow,Www-Authenticate`;
  headersObj['wac-allow'] = 'user="read",public="read"';
  headersObj.allow = 'OPTIONS, HEAD, GET, PATCH, POST';
  headersObj['accept-patch'] = 'text/n3, application/sparql-update';
  headersObj['accept-post'] = '*/*';
  headersObj['content-type'] = 'text/turtle';
  headersObj['last-modified'] = 'Fri, 17 Nov 2023 10:06:41 GMT';
  headersObj.etag = '"1700215601000-text/turtle"';
  headersObj.date = 'Fri, 17 Nov 2023 17:15:00 GMT';
  headersObj.connection = 'close';

  const headers = new Headers();

  for (const key in headersObj) {
    headers.set(key, headersObj[key]);
  }

  return {
    body: new PassThrough(),
    headers,
    status: 200,
    statusText: 'OK',
    ok: true,
    url,
    clone: jest.fn(),
    error: jest.fn(),
    json: () => ({}),
    redirect: jest.fn(),
    text: () => '',
  };
}

export function createResourceRequest(url: string) {
  const headersObj: any = {};
  headersObj.vary = 'Accept,Authorization,Origin';
  headersObj['x-powered-by'] = 'Community Solid Server';
  headersObj['access-control-allow-origin'] = '*';
  headersObj['access-control-allow-credentials'] = 'true';
  headersObj['access-control-expose-headers'] = `Accept-Patch,Accept-Post,Accept-Put,Allow,Content-Range,ETag,Last-Modified,Link,Location,Updates-Via,WAC-Allow,Www-Authenticate`;
  headersObj['wac-allow'] = 'user="read",public="read"';
  headersObj.allow = 'OPTIONS, HEAD, GET, PATCH, POST';
  headersObj['accept-patch'] = 'text/n3, application/sparql-update';
  headersObj['accept-post'] = '*/*';
  headersObj['content-type'] = 'text/turtle';
  headersObj.link =
    '<http://www.w3.org/ns/pim/space#Storage>; rel="type", <http://www.w3.org/ns/ldp#Container>; rel="type", <http://www.w3.org/ns/ldp#BasicContainer>; rel="type", <http://www.w3.org/ns/ldp#Resource>; rel="type", <http://localhost:3000/pod1/.meta>; rel="describedby", <http://localhost:3000/pod1/.acl>; rel="acl", <http://localhost:3000/.well-known/solid>; rel="http://www.w3.org/ns/solid/terms#storageDescription"';
  headersObj['last-modified'] = 'Fri, 17 Nov 2023 10:06:41 GMT';
  headersObj.etag = '"1700215601000-text/turtle"';
  headersObj.date = 'Fri, 17 Nov 2023 17:15:00 GMT';
  headersObj.connection = 'close';

  const headers = new Headers();

  for (const key in headersObj) {
    headers.set(key, headersObj[key]);
  }

  return {
    body: new PassThrough(),
    headers,
    status: 200,
    statusText: 'OK',
    ok: true,
    url,
    clone: jest.fn(),
    error: jest.fn(),
    json: () => ({}),
    redirect: jest.fn(),
    text: () => '',
  };
}

export function createDescriptionResourceRequest(url: string) {
  const headersObj: any = {};
  headersObj.vary = 'Accept,Authorization,Origin';
  headersObj['x-powered-by'] = 'Community Solid Server';
  headersObj['access-control-allow-origin'] = '*';
  headersObj['access-control-allow-credentials'] = 'true';
  headersObj['access-control-expose-headers'] = `Accept-Patch,Accept-Post,Accept-Put,Allow,Content-Range,ETag,Last-Modified,Link,Location,Updates-Via,WAC-Allow,Www-Authenticate`;
  headersObj['content-type'] = 'text/turtle';
  headersObj.etag = '"1700215601000-text/turtle"';
  headersObj.date = 'Fri, 17 Nov 2023 17:15:00 GMT';
  headersObj.connection = 'close';
  headersObj['transfer-encoding'] = 'chunked';

  const headers = new Headers();

  for (const key in headersObj) {
    headers.set(key, headersObj[key]);
  }

  const body = `
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
notify:state .
`;

  return {
    body: new PassThrough({
      read() {
        this.push(body);
        this.push(null);
      },
    }),
    headers,
    status: 200,
    statusText: 'OK',
    ok: true,
    url,
    clone: jest.fn(),
    error: jest.fn(),
    json: () => ({}),
    redirect: jest.fn(),
    text: () => body,
  };
}

export function createChannelDescriptionRequest(url: string) {
  const headersObj: any = {};
  headersObj.vary = 'Accept,Authorization,Origin';
  headersObj['x-powered-by'] = 'Community Solid Server';
  headersObj['access-control-allow-origin'] = '*';
  headersObj['access-control-allow-credentials'] = 'true';
  headersObj['access-control-expose-headers'] = `Accept-Patch,Accept-Post,Accept-Put,Allow,Content-Range,ETag,Last-Modified,Link,Location,Updates-Via,WAC-Allow,Www-Authenticate`;
  headersObj['content-type'] = 'text/turtle';
  headersObj.date = 'Fri, 17 Nov 2023 17:15:00 GMT';
  headersObj.connection = 'close';
  headersObj['transfer-encoding'] = 'chunked';

  const headers = new Headers();

  for (const key in headersObj) {
    headers.set(key, headersObj[key]);
  }

  const body = `
<> <http://www.w3.org/ns/solid/notifications#topic> <http://www.test.com>;
    <http://www.w3.org/ns/solid/notifications#receiveFrom> <ws://localhost:4015>;
    <http://www.w3.org/ns/solid/notifications#endAt> "2023-12-02T11:36:15.651Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
    a <http://www.w3.org/ns/solid/notifications#WebSocketChannel2023>.
`;

  return {
    body: new PassThrough({
      read() {
        this.push(body);
        this.push(null);
      },
    }),
    headers,
    status: 200,
    statusText: 'OK',
    ok: true,
    url,
    clone: jest.fn(),
    error: jest.fn(),
    json: () => ({}),
    redirect: jest.fn(),
    text: () => body,
  };
}

export function createEmptyChannelDescriptionRequest(url: string) {
  const headersObj: any = {};
  headersObj.vary = 'Accept,Authorization,Origin';
  headersObj['x-powered-by'] = 'Community Solid Server';
  headersObj['access-control-allow-origin'] = '*';
  headersObj['access-control-allow-credentials'] = 'true';
  headersObj['access-control-expose-headers'] = `Accept-Patch,Accept-Post,Accept-Put,Allow,Content-Range,ETag,Last-Modified,Link,Location,Updates-Via,WAC-Allow,Www-Authenticate`;
  headersObj['content-type'] = 'text/turtle';
  headersObj.date = 'Fri, 17 Nov 2023 17:15:00 GMT';
  headersObj.connection = 'close';
  headersObj['transfer-encoding'] = 'chunked';

  const headers = new Headers();

  for (const key in headersObj) {
    headers.set(key, headersObj[key]);
  }

  const body = `
<efc229f5-5d7c-41d0-9063-990ac364cd22> <http://www.w3.org/ns/solid/notifications#topic> <http://www.test.com>;
    <http://www.w3.org/ns/solid/notifications#endAt> "2023-12-02T11:36:15.651Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>;
    a <http://www.w3.org/ns/solid/notifications#WebSocketChannel2023>.
`;

  return {
    body: new PassThrough({
      read() {
        this.push(body);
        this.push(null);
      },
    }),
    headers,
    status: 200,
    statusText: 'OK',
    ok: true,
    url,
    clone: jest.fn(),
    error: jest.fn(),
    json: () => ({}),
    redirect: jest.fn(),
    text: () => body,
  };
}
