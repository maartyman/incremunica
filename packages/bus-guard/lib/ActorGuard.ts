import type { IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Actor } from '@comunica/core';
import type { IGuardEvents } from '@incremunica/incremental-types/lib/GuardEvents';
import type { StreamingQuerySource } from '@incremunica/streaming-query-source';

/**
 * A comunica actor for guard events.
 *
 * Actor Guard:
 * * Input:  IActionGuard:      // TODO [2024-12-01]: fill in.
 * * Test:   <none>
 * * Output: IActorGuardOutput: // TODO [2024-12-01]: fill in.
 *
 * @see IActionGuard
 * @see IActorGuardOutput
 */
export abstract class ActorGuard<TS = undefined> extends Actor<IActionGuard, IActorTest, IActorGuardOutput, TS> {
  /**
   * @param args - @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   */
  public constructor(args: IActorGuardArgs<TS>) {
    super(args);
  }
}

export interface IActionGuard extends IAction {
  /**
   * The source element of the data.
   */
  streamingQuerySource: StreamingQuerySource;
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

export type IActorGuardArgs<TS = undefined> = IActorArgs<
IActionGuard,
IActorTest,
IActorGuardOutput,
TS
>;

export type MediatorGuard = Mediate<
IActionGuard,
IActorGuardOutput
>;
