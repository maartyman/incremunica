import { Actor, IAction, IActorArgs, IActorOutput, IActorTest, Mediate } from '@comunica/core';
import {RdfJsQuadStreamSource} from "@comunica/actor-rdf-resolve-hypermedia-stream-source/lib/RdfJsQuadStreamSource";

/**
 * A comunica actor for guard events.
 *
 * Actor types:
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
  /*
  HttpHeaders: Headers;
  */
  /**
   * The URL of the source that was fetched.
   */
  streamSource: RdfJsQuadStreamSource;
}

export interface IActorGuardOutput extends IActorOutput {

}

export type IActorGuardArgs = IActorArgs<
IActionGuard, IActorTest, IActorGuardOutput>;

export type MediatorGuard = Mediate<
IActionGuard, IActorGuardOutput>;
