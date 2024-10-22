import { EventEmitter } from 'node:events';
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
    const checkForChanges = async(): Promise<void> => {
      // TODO maybe add a log if something goes wrong
      const responseHead = await this.mediatorHttp.mediate(
        {
          context: action.context,
          input: action.url,
          init: {
            method: 'HEAD',
          },
        },
      );

      if (!responseHead.ok) {
        events.emit('delete');
      }

      if (responseHead.headers.get('etag') !== etag) {
        events.emit('update');
        etag = responseHead.headers.get('etag');
      }
    };

    let timeoutId: NodeJS.Timeout | undefined;

    const startCheckLoop = (maxAge: number): void => {
      timeoutId = setInterval(
        () => {
          checkForChanges().catch(() => {});
        },
        maxAge * 1_000,
      );
    };

    const maxAgeArray = this.regex.exec(action.metadata['cache-control']);

    let pollingFrequency: number;
    if (maxAgeArray) {
      pollingFrequency = Number.parseInt(maxAgeArray[1], 10);
    } else {
      pollingFrequency = this.defaultPollingFrequency;
    }

    const age = Number.parseInt(action.metadata.age, 10);
    if (age) {
      // eslint-disable-next-line ts/no-floating-promises
      new Promise<void>((resolve) => {
        timeoutId = setTimeout(
          () => {
            checkForChanges().then(
              () => resolve(),
            ).catch(
              () => resolve(),
            );
          },
          (pollingFrequency - age) * 1_000,
        );
      }).then(() => {
        startCheckLoop(pollingFrequency);
      });
    } else {
      startCheckLoop(pollingFrequency);
    }

    return {
      events,
      stopFunction(): void {
        events.removeAllListeners();
        if (timeoutId) {
          clearInterval(timeoutId);
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
