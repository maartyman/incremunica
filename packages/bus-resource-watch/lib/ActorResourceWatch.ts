import type { EventEmitter } from 'node:events';
import type { IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Actor } from '@comunica/core';

/**
 * An incremunica actor for resource-watch events.
 *
 * Actor types:
 * * Input:  IActionResourceWatch:      TODO: fill in.
 * * Test:   <none>
 * * Output: IActorResourceWatchOutput: TODO: fill in.
 *
 * @see IActionResourceWatch
 * @see IActorResourceWatchOutput
 */
export abstract class ActorResourceWatch<TS = undefined> extends Actor<
  IActionResourceWatch,
  IActorTest,
  IActorResourceWatchOutput,
  TS
> {
  public readonly priority: number;
  /**
   * @param args - @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   */
  public constructor(args: IActorResourceWatchArgs<TS>) {
    super(args);
  }
}

export declare interface IResourceWatchEventEmitter extends EventEmitter {
  emit: ((event: 'update') => boolean) & ((event: 'delete') => boolean);
  on: ((event: 'update', listener: () => void) => this) & ((event: 'delete', listener: () => void) => this);
}

export interface IActionResourceWatch extends IAction {
  /**
   * The URL of the source that was fetched.
   */
  url: string;
  /**
   * The extracted metadata.
   */
  metadata: Record<string, any>;
}

export interface IActorResourceWatchOutput extends IActorOutput {
  /**
   * An event emitter that emits 'update' and 'delete' events.
   */
  events: IResourceWatchEventEmitter;
  /**
   * A function to stop watching the resource.
   */
  stopFunction: () => void;
}

export interface IActorResourceWatchArgs<TS = undefined> extends IActorArgs<
IActionResourceWatch,
IActorTest,
IActorResourceWatchOutput,
TS
> {
  /**
   * The priority of the actor.
   */
  priority: number;
}

export type MediatorResourceWatch = Mediate<
IActionResourceWatch,
IActorResourceWatchOutput
>;
