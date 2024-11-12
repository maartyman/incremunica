import { EventEmitter } from 'events';
import type { MediatorHttp } from '@comunica/bus-http';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestWithSideData } from '@comunica/core';
import type {
  IActionResourceWatch,
  IActorResourceWatchArgs,
  IActorResourceWatchOutput,
  IResourceWatchEventEmitter,
} from '@incremunica/bus-resource-watch';
import {
  ActorResourceWatch,
} from '@incremunica/bus-resource-watch';
import { SubscriptionClient } from '@solid-notifications/subscription';
import type { NotificationChannel } from '@solid-notifications/types';
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

    let notificationChannel: NotificationChannel;
    try {
      const client = new SubscriptionClient(<typeof fetch>customFetch);
      notificationChannel = await client.subscribe(action.url, this.channelType);
    } catch (error) {
      return failTest((<any>error).message);
    }

    if (notificationChannel.receiveFrom === undefined) {
      return failTest('Resource does not support Solid Notifications with Websockets');
    }

    return passTestWithSideData({ priority: this.priority }, { notificationChannel: notificationChannel.receiveFrom });
  }

  public async run(
    _action: IActionResourceWatch,
    sideData: SideData,
  ): Promise<IActorResourceWatchOutput> {
    const events: IResourceWatchEventEmitter = new EventEmitter();

    let socket: WebSocket | undefined;
    const start = (): void => {
      if (socket) {
        return;
      }
      socket = new WebSocket(sideData.notificationChannel);
      socket.onmessage = (message) => {
        // TODO [2024-12-01]: For now ignoring the Buffer options => tests?
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
      socket.onopen = () => {
        events.emit('update');
      };
    };

    return {
      events,
      stop() {
        if (socket) {
          socket.close();
        }
        socket = undefined;
      },
      start,
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
