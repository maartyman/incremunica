import type { IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Actor } from '@comunica/core';
import type { IGuardEvents } from '@incremunica/incremental-types/lib/GuardEvents';
import {IQuerySource} from "@comunica/types";
import {StreamingQuerySourceRdfJs} from "@incremunica/actor-query-source-identify-streaming-rdfjs";

/**
 * A comunica actor for guard events.
 *
 * Actor Guard:
 * * Input:  IActionGuard:      TODO: fill in.
 * * Test:   <none>
 * * Output: IActorGuardOutput: TODO: fill in.
 *
 * @see IActionGuard
 * @see IActorGuardOutput
 */
export abstract class ActorGuard extends Actor<IActionGuard, IActorTest, IActorGuardOutput> {
  /**
   * @param args - @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   */
  public constructor(args: IActorGuardArgs) {
    super(args);
  }
}

export interface IActionGuard extends IAction {
  /**
   * The source element of the data.
   */
  streamingSource: StreamingQuerySourceRdfJs;
  /**
   * The URL of the source that was fetched.
   */
  url: string;
  /**
   * The extracted metadata.
   */
  metadata: Record<string, any>;
}

export interface IActorGuardOutput extends IActorOutput {
  /**
   * Events send by the guard
   */
  guardEvents: IGuardEvents;
}

export type IActorGuardArgs = IActorArgs<
IActionGuard, IActorTest, IActorGuardOutput>;

export type MediatorGuard = Mediate<
IActionGuard, IActorGuardOutput>;

