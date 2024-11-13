import { EventEmitter } from 'events';
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
 * An incremunica Deffered Resource Watch Actor.
 */
export class ActorResourceWatchDeferred extends ActorResourceWatch {
  public constructor(args: IActorResourceWatchArgs) {
    super(args);
  }

  public async test(action: IActionResourceWatch): Promise<TestResult<IActorTest>> {
    if (!action.context.has(KeysResourceWatch.deferredEvaluationEventEmitter)) {
      return failTest('Context does not have \'deferredEvaluationEventEmitter\'');
    }
    return passTest({ priority: this.priority });
  }

  public async run(action: IActionResourceWatch): Promise<IActorResourceWatchOutput> {
    const eventsSource: IResourceWatchEventEmitter = action.context
      .getSafe(KeysResourceWatch.deferredEvaluationEventEmitter);
    const outputEvents: IResourceWatchEventEmitter = new EventEmitter();

    const emitUpdate = (): void => {
      outputEvents.emit('update');
    };
    let running = false;
    return {
      events: outputEvents,
      start: () => {
        if (!running) {
          running = true;
          eventsSource.on('update', emitUpdate);
        }
      },
      stop: () => {
        if (running) {
          running = false;
          eventsSource.removeListener('update', emitUpdate);
        }
      },
    };
  }
}
