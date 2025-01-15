import type { EventEmitter } from 'events';
import type { IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Actor } from '@comunica/core';

/**
 * An incremunica actor for source-watch events.
 *
 * Actor types:
 * * Input:  IActionSourceWatch:      // TODO: fill in.
 * * Test:   <none>
 * * Output: IActorSourceWatchOutput: // TODO: fill in.
 *
 * @see IActionSourceWatch
 * @see IActorSourceWatchOutput
 */
export abstract class ActorSourceWatch<TS = undefined> extends Actor<
  IActionSourceWatch,
  IActorTest,
  IActorSourceWatchOutput,
  TS
> {
  public readonly priority: number;
  /**
   * @param args - @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   */
  public constructor(args: IActorSourceWatchArgs<TS>) {
    super(args);
  }
}

export declare interface ISourceWatchEventEmitter extends EventEmitter {
  emit: ((event: 'update') => boolean) & ((event: 'delete') => boolean);
  on: ((event: 'update', listener: () => void) => this) & ((event: 'delete', listener: () => void) => this);
}

export interface IActionSourceWatch extends IAction {
  /**
   * The URL of the source that was fetched.
   */
  url: string;
  /**
   * The extracted metadata.
   */
  metadata: Record<string, any>;
}

export interface IActorSourceWatchOutput extends IActorOutput {
  /**
   * An event emitter that emits 'update' and 'delete' events.
   */
  events: ISourceWatchEventEmitter;
  /**
   * A function to stop watching the source.
   */
  stop: () => void;
  /**
   * A function to start watching the source.
   */
  start: () => void;
}

export interface IActorSourceWatchArgs<TS = undefined> extends IActorArgs<
IActionSourceWatch,
IActorTest,
IActorSourceWatchOutput,
TS
> {
  /**
   * The priority of the actor.
   */
  priority: number;
}

export type MediatorSourceWatch = Mediate<
IActionSourceWatch,
IActorSourceWatchOutput
>;
