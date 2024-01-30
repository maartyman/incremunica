import { EventEmitter } from 'events';
import type { MediatorHttp } from '@comunica/bus-http';
import type { IActorTest } from '@comunica/core';
import type {
  IActionResourceWatch, IActorResourceWatchArgs,
  IActorResourceWatchOutput,
  IResourceWatchEventEmitter,
} from '@incremunica/bus-resource-watch';
import {
  ActorResourceWatch,
} from '@incremunica/bus-resource-watch';
import { SubscriptionClient } from '@solid-notifications/subscription';
import { ChannelType } from '@solid-notifications/types';
// eslint-disable-next-line import/no-unassigned-import
import 'websocket-polyfill';

/**
 * An incremunica Resource Watch Solid Notification Websockets Actor.
 */
export class ActorResourceWatchSolidNotificationWebsockets extends ActorResourceWatch {
  public readonly mediatorHttp: MediatorHttp;
  private readonly channelType: ChannelType = ChannelType.WebSocketChannel2023;

  public constructor(args: IActorSolidNotificationWebsocketsArgs) {
    super(args);
  }

  public async test(action: IActionResourceWatch): Promise<IActorTest> {
    const customFetch = (input: RequestInfo, init?: RequestInit | undefined): Promise<Response> =>
      this.mediatorHttp.mediate({
        context: action.context,
        input,
        init,
      });

    const client = new SubscriptionClient(<typeof fetch>customFetch);
    const notificationChannel = await client.subscribe(action.url, this.channelType);

    if (notificationChannel.receiveFrom === undefined) {
      throw new Error('Resource does not support Solid Notifications with Websockets');
    }

    return { priority: this.priority };
  }

  public async run(action: IActionResourceWatch): Promise<IActorResourceWatchOutput> {
    const customFetch = (input: RequestInfo, init?: RequestInit | undefined): Promise<Response> =>
      this.mediatorHttp.mediate({
        context: action.context,
        input,
        init,
      });

    const client = new SubscriptionClient(<typeof fetch>customFetch);
    const notificationChannel = await client.subscribe(action.url, this.channelType);

    if (notificationChannel.receiveFrom === undefined) {
      throw new Error('No receiveFrom in notificationChannel');
    }

    const socket = new WebSocket(notificationChannel.receiveFrom);

    const events: IResourceWatchEventEmitter = new EventEmitter();

    socket.onmessage = message => {
      // TODO: For now ignoring the Buffer options => tests?
      // let data: string | Buffer | ArrayBuffer | Buffer[] = message.data;
      // if (Array.isArray(data)) {
      // data = Buffer.concat(data);
      // }
      // if (data instanceof Buffer) {
      // data = data.toString();
      // }
      // if (data instanceof ArrayBuffer) {
      // const decoder = new TextDecoder('utf-8');
      // data = decoder.decode(data);
      // }

      const messageData = JSON.parse(<string>message.data);
      if (messageData.type === 'Delete') {
        events.emit('delete');
      } else {
        events.emit('update');
      }
    };

    return {
      events,
      stopFunction() {
        events.removeAllListeners();
        socket.close();
      },
    };
  }
}

export interface IActorSolidNotificationWebsocketsArgs extends IActorResourceWatchArgs {
  /**
   * The HTTP mediator
   */
  mediatorHttp: MediatorHttp;
}
