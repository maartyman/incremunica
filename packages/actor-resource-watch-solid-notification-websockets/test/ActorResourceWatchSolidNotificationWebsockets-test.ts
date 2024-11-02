import type { IActionHttp, IActorHttpOutput } from '@comunica/bus-http';
import type { Actor, IActorTest, Mediator } from '@comunica/core';
import { Bus } from '@comunica/core';
import 'jest-rdf';
import '@comunica/utils-jest';
import type { IActionResourceWatch } from '@incremunica/bus-resource-watch';
import type { Server } from 'ws';
import { WebSocket } from 'ws';
import { ActorResourceWatchSolidNotificationWebsockets } from '../lib/ActorResourceWatchSolidNotificationWebsockets';
import {
  createChannelDescriptionRequest,
  createDescriptionResourceRequest,
  createEmptyChannelDescriptionRequest,
  createEmptyResourceRequest,
  createFailedRequest,
  createResourceRequest,
} from './mocks/HTTPMock';

const message: any = {
  '@context': [
    'https://www.w3.org/ns/activitystreams',
    'https://www.w3.org/ns/solid/notification/v1',
  ],
  id: 'urn:1700310987535:http://localhost:3000/pod1/container/',
  object: 'http://localhost:3000/pod1/container/test',
  target: 'http://localhost:3000/pod1/container/',
  published: '2023-11-18T12:36:27.535Z',
};

//
// let AddMessage = {
// "type": "Add",
// "object": "http://localhost:3000/pod1/test",
// "target": "http://localhost:3000/pod1/",
// "state": "1700310987000-text/turtle",
// };
//
// let RemoveMessage = {
// "type":"Remove",
// "object":"http://localhost:3000/pod1/test",
// "target":"http://localhost:3000/pod1/",
// "state":"1700311266000-text/turtle",
// }
//
// let UpdateMessage = {
// "type":"Update",
// "object":"http://localhost:3000/pod1/test",
// "state":"1700311389000-text/turtle",
// };
//
// let DeleteMessage = {
// "type":"Delete",
// "object":"http://localhost:3000/pod1/test2",
// };
//
// let CreateMessage = {
// "type":"Create",
// "object":"http://localhost:3000/pod1/test2",
// "state":"1700311525000-text/turtle",
// };
//

describe('ActorResourceWatchSolidNotificationWebsockets', () => {
  let bus: any;
  let actor: ActorResourceWatchSolidNotificationWebsockets;
  let mediatorHttp: Mediator<
    Actor<IActionHttp, IActorTest, IActorHttpOutput>,
    IActionHttp,
IActorTest,
IActorHttpOutput
>;
  let action: IActionResourceWatch;
  let priority: number;
  let createResourceRequestFn: (url: string) => any;
  let createDescriptionResourceRequestFn: (url: string) => any;
  let createChannelDescriptionRequestFn: (url: string) => any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    priority = 0;

    createResourceRequestFn = (url: string): any => {
      throw new Error('createResourceRequestFn not set');
    };
    createDescriptionResourceRequestFn = (url: string): any => {
      throw new Error('createDescriptionResourceRequestFn not set');
    };
    createChannelDescriptionRequestFn = (url: string): any => {
      throw new Error('createChannelDescriptionRequestFn not set');
    };

    mediatorHttp = <any>{
      mediate: async(action: IActionHttp) => {
        if (action.input === 'www.test.com') {
          return createResourceRequestFn('www.test.com');
        }
        if (action.input === 'http://localhost:3000/.well-known/solid') {
          return createDescriptionResourceRequestFn(action.input);
        }
        if (action.input === 'http://localhost:3000/.notifications/WebSocketChannel2023/') {
          return createChannelDescriptionRequestFn(action.input);
        }
        throw new Error('Unknown URL');
      },
    };

    actor = new ActorResourceWatchSolidNotificationWebsockets({
      beforeActors: [],
      mediatorHttp,
      name: 'actor',
      bus,
      priority,
    });

    action = {
      context: <any>{},
      url: 'www.test.com',
      metadata: {
        etag: 0,
        'cache-control': undefined,
        age: undefined,
      },
    };
  });

  describe('ActorResourceWatchSolidNotificationWebsockets test', () => {
    it('should test', async() => {
      createResourceRequestFn = createResourceRequest;
      createDescriptionResourceRequestFn = createDescriptionResourceRequest;
      createChannelDescriptionRequestFn = createChannelDescriptionRequest;

      const result = await actor.test(action);

      expect(result.get()).toEqual({ priority });
    });

    it('should not test if first get fails', async() => {
      createResourceRequestFn = createFailedRequest;
      createDescriptionResourceRequestFn = createDescriptionResourceRequest;
      createChannelDescriptionRequestFn = createChannelDescriptionRequest;

      await expect(actor.test(action)).resolves.toFailTest('Failed fetching resource: www.test.com');
    });

    it('should not test if second get fails', async() => {
      createResourceRequestFn = createResourceRequest;
      createDescriptionResourceRequestFn = createFailedRequest;
      createChannelDescriptionRequestFn = createChannelDescriptionRequest;

      await expect(actor.test(action)).resolves
        .toFailTest('Failed fetching resource: http://localhost:3000/.well-known/solid');
    });

    it('should not test if third get fails', async() => {
      createResourceRequestFn = createResourceRequest;
      createDescriptionResourceRequestFn = createDescriptionResourceRequest;
      createChannelDescriptionRequestFn = createFailedRequest;

      await expect(actor.test(action)).resolves.toFailTest('unexpected Content Type');
    });

    it('should not test if subscriptionService is undefined', async() => {
      createResourceRequestFn = createEmptyResourceRequest;
      createDescriptionResourceRequestFn = createDescriptionResourceRequest;
      createChannelDescriptionRequestFn = createChannelDescriptionRequest;

      await expect(actor.test(action)).resolves.toFailTest('Cannot read properties of null (reading \'replace\')');
    });

    it('should not test if notificationChannel.receiveFrom is undefined', async() => {
      createResourceRequestFn = createResourceRequest;
      createDescriptionResourceRequestFn = createDescriptionResourceRequest;
      createChannelDescriptionRequestFn = createEmptyChannelDescriptionRequest;

      await expect(actor.test(action))
        .resolves.toFailTest('Resource does not support Solid Notifications with Websockets');
    });
  });

  describe('ActorResourceWatchSolidNotificationWebsockets run', () => {
    let websocket: Server<typeof import('ws')>;

    beforeEach(() => {
      websocket = new WebSocket.WebSocketServer({ port: 4015 });
    });

    afterEach(() => {
      websocket.close();
    });

    it('should support ADD', async() => {
      createResourceRequestFn = createResourceRequest;
      createDescriptionResourceRequestFn = createDescriptionResourceRequest;
      createChannelDescriptionRequestFn = createChannelDescriptionRequest;

      const result = await actor.run(action, { notificationChannel: 'ws://localhost:4015' });

      websocket.on('connection', (ws: WebSocket) => {
        message.type = 'Add';
        ws.send(JSON.stringify(message));
      });

      await new Promise<void>(resolve => result.events.on('update', () => {
        resolve();
      }));

      expect(true).toBeTruthy();

      result.stopFunction();
    });

    it('should support Remove', async() => {
      createResourceRequestFn = createResourceRequest;
      createDescriptionResourceRequestFn = createDescriptionResourceRequest;
      createChannelDescriptionRequestFn = createChannelDescriptionRequest;

      const result = await actor.run(action, { notificationChannel: 'ws://localhost:4015' });

      websocket.on('connection', (ws: WebSocket) => {
        message.type = 'Remove';
        ws.send(JSON.stringify(message));
      });

      await new Promise<void>(resolve => result.events.on('update', () => {
        resolve();
      }));

      expect(true).toBeTruthy();

      result.stopFunction();
    });

    it('should support Create', async() => {
      createResourceRequestFn = createResourceRequest;
      createDescriptionResourceRequestFn = createDescriptionResourceRequest;
      createChannelDescriptionRequestFn = createChannelDescriptionRequest;

      const result = await actor.run(action, { notificationChannel: 'ws://localhost:4015' });

      websocket.on('connection', (ws: WebSocket) => {
        message.type = 'Create';
        ws.send(JSON.stringify(message));
      });

      await new Promise<void>(resolve => result.events.on('update', () => {
        resolve();
      }));

      expect(true).toBeTruthy();

      result.stopFunction();
    });

    it('should support Update', async() => {
      createResourceRequestFn = createResourceRequest;
      createDescriptionResourceRequestFn = createDescriptionResourceRequest;
      createChannelDescriptionRequestFn = createChannelDescriptionRequest;

      const result = await actor.run(action, { notificationChannel: 'ws://localhost:4015' });

      websocket.on('connection', (ws: WebSocket) => {
        message.type = 'Update';
        ws.send(JSON.stringify(message));
      });

      await new Promise<void>(resolve => result.events.on('update', () => {
        resolve();
      }));

      expect(true).toBeTruthy();

      result.stopFunction();
    });

    it('should support Delete', async() => {
      createResourceRequestFn = createResourceRequest;
      createDescriptionResourceRequestFn = createDescriptionResourceRequest;
      createChannelDescriptionRequestFn = createChannelDescriptionRequest;

      const result = await actor.run(action, { notificationChannel: 'ws://localhost:4015' });

      websocket.on('connection', (ws: WebSocket) => {
        message.type = 'Delete';
        ws.send(JSON.stringify(message));
      });

      await new Promise<void>(resolve => result.events.on('delete', () => {
        resolve();
      }));

      expect(true).toBeTruthy();

      result.stopFunction();
    });

    // TODO re-enable these tests
    // eslint-disable-next-line jest/no-commented-out-tests
    // it('should support Buffer messages', async () => {
    // createResourceRequestFn = createResourceRequest;
    // createDescriptionResourceRequestFn = createDescriptionResourceRequest;
    // createChannelDescriptionRequestFn = createChannelDescriptionRequest;
    //
    // let result = await actor.run(action);
    //
    // websocket.on('connection', (ws: WebSocket) => {
    //     message["type"] = "Delete";
    //     ws.send(Buffer.from(JSON.stringify(message)));
    // });
    //
    // await new Promise<void>(resolve => result.events.on("delete", () => {
    //     resolve();
    // }));
    //
    // expect(true).toBeTruthy();
    //
    // result.stopFunction();
    // });

    // eslint-disable-next-line jest/no-commented-out-tests
    // it('should support Buffer[] messages', async () => {
    // createResourceRequestFn = createResourceRequest;
    // createDescriptionResourceRequestFn = createDescriptionResourceRequest;
    // createChannelDescriptionRequestFn = createChannelDescriptionRequest;
    //
    // let result = await actor.run(action);
    //
    // websocket.on('connection', (ws: WebSocket) => {
    //     message["type"] = "Delete";
    //     let start = JSON.stringify(message).substring(0,1)
    //     let end = JSON.stringify(message).substring(1)
    //     ws.send([Buffer.from(start), Buffer.from(end)]);
    // });
    //
    // await new Promise<void>(resolve => result.events.on("delete", () => {
    //     resolve();
    // }));
    //
    // expect(true).toBeTruthy();
    //
    // result.stopFunction();
    // });

    // eslint-disable-next-line jest/no-commented-out-tests
    // it('should support ArrayBuffer messages', async () => {
    // createResourceRequestFn = createResourceRequest;
    // createDescriptionResourceRequestFn = createDescriptionResourceRequest;
    // createChannelDescriptionRequestFn = createChannelDescriptionRequest;
    //
    // let result = await actor.run(action);
    //
    // function stringToUint(string: string) {
    //     string = JSON.stringify(string);
    //     let charList = string.split('');
    //     let uintArray = [];
    //     for (let i = 0; i < charList.length; i++) {
    //       uintArray.push(charList[i].charCodeAt(0));
    //     }
    //     return new Uint8Array(uintArray);
    // }
    //
    // websocket.on('connection', (ws: WebSocket) => {
    //     message["type"] = "Delete";
    //     ws.send(stringToUint(message));
    // });
    //
    // await new Promise<void>(resolve => result.events.on("delete", () => {
    //     resolve();
    // }));
    //
    // expect(true).toBeTruthy();
    //
    // result.stopFunction();
    // });
  });
});
