import { EventEmitter } from 'node:events';
import type { MediatorHttp } from '@comunica/bus-http';
import {failTest, IActorTest, passTestWithSideData, TestResult} from '@comunica/core';
import {
  ActorResourceWatch,
  IActionResourceWatch, IActorResourceWatchArgs,
  IActorResourceWatchOutput,
  IResourceWatchEventEmitter,
} from '@incremunica/bus-resource-watch';
import { SubscriptionClient } from '@solid-notifications/subscription';
import { ChannelType } from '@solid-notifications/types';

import 'websocket-polyfill';

/**
 * An incremunica Resource Watch Solid Notification Websockets Actor.
 */
export class ActorResourceWatchSolidNotificationWebsockets extends ActorResourceWatch<SideData> {
  public readonly mediatorHttp: MediatorHttp;
  private readonly channelType: ChannelType = ChannelType.WebSocketChannel2023;

  public constructor(args: IActorSolidNotificationWebsocketsArgs) {
    super(args);
  }

  public async test(action: IActionResourceWatch): Promise<TestResult<IActorTest, SideData>> {
    const customFetch = (input: RequestInfo, init?: RequestInit | undefined): Promise<Response> =>
      this.mediatorHttp.mediate({
        context: action.context,
        input,
        init,
      });

    const client = new SubscriptionClient(<typeof fetch>customFetch);
    const notificationChannel = await client.subscribe(action.url, this.channelType);

    if (notificationChannel.receiveFrom === undefined) {
      return failTest('Resource does not support Solid Notifications with Websockets');
    }

    return passTestWithSideData({ priority: this.priority }, { notificationChannel: notificationChannel.receiveFrom });
  }

  public async run(
    _action: IActionResourceWatch,
    sideData: SideData
  ): Promise<IActorResourceWatchOutput> {
    const socket = new WebSocket(sideData.notificationChannel);

    const events: IResourceWatchEventEmitter = new EventEmitter();

    socket.onmessage = (message) => {
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

export interface IActorSolidNotificationWebsocketsArgs extends IActorResourceWatchArgs<SideData> {
  /**
   * The HTTP mediator
   */
  mediatorHttp: MediatorHttp;
}

interface SideData {
  notificationChannel: string;
}
