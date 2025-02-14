import { EventEmitter } from 'events';
import type { ISourceWatchEventEmitter } from '@incremunica/types';

/**
 * A helper function for deferred evaluation that can trigger the query engine to update.
 */
export class DeferredEvaluation {
  /**
   * The events to pass to the query engine.
   */
  public readonly events: ISourceWatchEventEmitter = new EventEmitter();

  /**
   * Trigger an update of the query engine.
   */
  public triggerUpdate(): void {
    this.events.emit('update');
  }
}
