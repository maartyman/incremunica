import { EventEmitter } from 'events';
import type { MediatorHttp } from '@comunica/bus-http';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTest } from '@comunica/core';
import type {
  IActionResourceWatch,
  IActorResourceWatchArgs,
  IActorResourceWatchOutput,
  IResourceWatchEventEmitter,
} from '@incremunica/bus-resource-watch';
import {
  ActorResourceWatch,
} from '@incremunica/bus-resource-watch';
import { KeysResourceWatch } from '@incremunica/context-entries';

/**
 * An incremunica Deferred Resource Watch Actor.
 */
export class ActorResourceWatchDeferred extends ActorResourceWatch {
  public readonly mediatorHttp: MediatorHttp;
  public constructor(args: IActorResourceWatchDeferredArgs) {
    super(args);
  }

  public async test(action: IActionResourceWatch): Promise<TestResult<IActorTest>> {
    if (!action.context.has(KeysResourceWatch.deferredEvaluationEventEmitter)) {
      return failTest('Context does not have \'deferredEvaluationEventEmitter\'');
    }
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
      return failTest('Source does not support HEAD requests');
    }
    if (!responseHead.headers.get('etag')) {
      return failTest('Source does not support etag headers');
    }
    return passTest({ priority: this.priority });
  }

  public async run(action: IActionResourceWatch): Promise<IActorResourceWatchOutput> {
    const eventsSource: IResourceWatchEventEmitter = action.context
      .getSafe(KeysResourceWatch.deferredEvaluationEventEmitter);
    eventsSource.setMaxListeners(eventsSource.getMaxListeners() + 1);
    const outputEvents: IResourceWatchEventEmitter = new EventEmitter();

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
        // TODO [2025-08-01]: have more specific error handling for example 304: Not Modified should not emit 'delete'
        if (!responseHead.ok) {
          outputEvents.emit('delete');
        }
        if (responseHead.headers.get('etag') !== etag) {
          outputEvents.emit('update');
          etag = responseHead.headers.get('etag');
        }
      }).catch(() => {
        outputEvents.emit('delete');
      });
    };

    let running = false;
    return {
      events: outputEvents,
      start: () => {
        if (!running) {
          running = true;
          eventsSource.on('update', checkForChanges);
        }
      },
      stop: () => {
        if (running) {
          running = false;
          eventsSource.removeListener('update', checkForChanges);
        }
      },
    };
  }
}

export interface IActorResourceWatchDeferredArgs extends IActorResourceWatchArgs {
  /**
   * The HTTP mediator
   */
  mediatorHttp: MediatorHttp;
}
