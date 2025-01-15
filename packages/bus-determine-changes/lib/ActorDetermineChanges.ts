import type { IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Actor } from '@comunica/core';
import type { IDetermineChangesEvents } from '@incremunica/incremental-types';
import type { StreamingQuerySource } from '@incremunica/streaming-query-source';

/**
 * A comunica actor for determine-changes events.
 *
 * Actor DetermineChanges:
 * * Input:  IActionDetermineChanges:      // TODO [2024-12-01]: fill in.
 * * Test:   <none>
 * * Output: IActorDetermineChangesOutput: // TODO [2024-12-01]: fill in.
 *
 * @see IActionDetermineChanges
 * @see IActorDetermineChangesOutput
 */
export abstract class ActorDetermineChanges<TS = undefined> extends Actor<
  IActionDetermineChanges,
  IActorTest,
  IActorDetermineChangesOutput,
TS
> {
  /**
   * @param args - @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   */
  public constructor(args: IActorDetermineChangesArgs<TS>) {
    super(args);
  }
}

export interface IActionDetermineChanges extends IAction {
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

export interface IActorDetermineChangesOutput extends IActorOutput {
  /**
   * Events send by the determine-changes
   */
  determineChangesEvents: IDetermineChangesEvents;
}

export type IActorDetermineChangesArgs<TS = undefined> = IActorArgs<
IActionDetermineChanges,
IActorTest,
IActorDetermineChangesOutput,
TS
>;

export type MediatorDetermineChanges = Mediate<
IActionDetermineChanges,
IActorDetermineChangesOutput
>;
