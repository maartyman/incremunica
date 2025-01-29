import type { IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Actor } from '@comunica/core';
import type { ISourceWatchEventEmitter } from '@incremunica/types';

/**
 * An Incremunica actor for source-watch events.
 *
 * Actor types:
 * * Input:   IActionSourceWatch: The url and metadata of the source to watch.
 * * Test:    IActionSourceWatch: The url and metadata of the source to watch.
 * * Output:  IActorSourceWatchOutput: start and stop functions for the source watch,
 * and an event emitter for 'update' and 'delete' events.
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
