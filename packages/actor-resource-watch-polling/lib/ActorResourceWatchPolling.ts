import { EventEmitter } from 'events';
import type { MediatorHttp } from '@comunica/bus-http';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTest } from '@comunica/core';
import type {
  IActionResourceWatch,
  IActorResourceWatchArgs,
  IActorResourceWatchOutput,
  IResourceWatchEventEmitter,
} from '@incremunica/bus-resource-watch';
import {
  ActorResourceWatch,
} from '@incremunica/bus-resource-watch';

/**
 * An incremunica Polling Resource Watch Actor.
 */
export class ActorResourceWatchPolling extends ActorResourceWatch {
  public readonly mediatorHttp: MediatorHttp;
  public readonly defaultPollingFrequency: number;

  private readonly regex = /max-age=(\d+)/u;

  public constructor(args: IActorResourceWatchPollingArgs) {
    super(args);
  }

  public async test(_action: IActionResourceWatch): Promise<TestResult<IActorTest>> {
    return passTest({ priority: this.priority });
  }

  public async run(action: IActionResourceWatch): Promise<IActorResourceWatchOutput> {
    const events: IResourceWatchEventEmitter = new EventEmitter();

    let etag = action.metadata.etag;
    const checkForChanges = (): void => {
      this.mediatorHttp.mediate(
        {
          context: action.context,
          input: action.url,
          init: {
            method: 'HEAD',
          },
        },
      ).then((responseHead) => {
        // TODO have more specific error handling for example 304: Not Modified should not emit 'delete'
        if (!responseHead.ok) {
          events.emit('delete');
        }

        if (responseHead.headers.get('etag') !== etag) {
          events.emit('update');
          etag = responseHead.headers.get('etag');
        }
      }).catch(() => {
        events.emit('delete');
      });
    };

    const maxAgeArray = this.regex.exec(action.metadata['cache-control']);
    let pollingFrequency: number;
    if (maxAgeArray) {
      pollingFrequency = Number.parseInt(maxAgeArray[1], 10);
    } else {
      pollingFrequency = this.defaultPollingFrequency;
    }

    let loopId: NodeJS.Timeout | undefined;
    const startCheckLoop = (): void => {
      checkForChanges();
      loopId = setInterval(
        checkForChanges,
        pollingFrequency * 1_000,
      );
    };

    let timeoutId: NodeJS.Timeout | undefined;
    const age = Number.parseInt(action.metadata.age, 10);
    if (age) {
      timeoutId = setTimeout(
        startCheckLoop,
        (pollingFrequency - age) * 1_000,
      );
    } else {
      timeoutId = setTimeout(
        startCheckLoop,
        pollingFrequency * 1_000,
      );
    }

    return {
      events,
      stopFunction(): void {
        events.removeAllListeners();
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
        if (loopId) {
          clearInterval(loopId);
        }
      },
    };
  }
}

export interface IActorResourceWatchPollingArgs extends IActorResourceWatchArgs {
  /**
   * The HTTP mediator
   */
  mediatorHttp: MediatorHttp;
  /**
   * The Polling Frequency in seconds
   */
  defaultPollingFrequency: number;
}
