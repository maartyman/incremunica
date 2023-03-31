import type { RdfJsQuadStreamingSource } from '@comunica/actor-rdf-resolve-quad-pattern-rdfjs-streaming-source';
import type { IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import { Actor } from '@comunica/core';

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
  private static readonly guards: Map<string, IGuard> = new Map<string, IGuard>();

  /**
   * @param args - @defaultNested {<default_bus> a <cc:components/Bus.jsonld#Bus>} bus
   */
  public constructor(args: IActorGuardArgs) {
    super(args);
  }

  public static addGuard(url: string, guard: IGuard): void {
    const originalGuard = ActorGuard.guards.get(url);
    if (originalGuard) {
      originalGuard.delete(url);
    }
    ActorGuard.guards.set(url, guard);
  }

  public static getGuard(url: string): IGuard | undefined {
    return ActorGuard.guards.get(url);
  }

  public static deleteGuard(url: string): void {
    ActorGuard.guards.get(url)?.delete(url);
    ActorGuard.guards.delete(url);
  }
}

export interface IGuard {
  delete: (url: string) => void;
}

export interface IActionGuard extends IAction {
  /**
   * The source element of the data.
   */
  streamingSource: RdfJsQuadStreamingSource;
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

}

export type IActorGuardArgs = IActorArgs<
IActionGuard, IActorTest, IActorGuardOutput>;

export type MediatorGuard = Mediate<
IActionGuard, IActorGuardOutput>;

