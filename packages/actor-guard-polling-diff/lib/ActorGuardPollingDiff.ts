import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type { IActionGuard, IActorGuardOutput, IActorGuardArgs } from '@comunica/bus-guard';
import { ActorGuard } from '@comunica/bus-guard';
import type { MediatorHttp } from '@comunica/bus-http';
import type { IActorTest } from '@comunica/core';
import { PollingDiffGuard } from './PollingDiffGuard';

/**
 * A comunica Polling Diff Guard Actor.
 */
export class ActorGuardPollingDiff extends ActorGuard {
  public readonly mediatorHttp: MediatorHttp;
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;
  public readonly pollingFrequency: number;

  public constructor(args: IActorGuardPollingArgs) {
    super(args);
  }

  public async test(action: IActionGuard): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionGuard): Promise<IActorGuardOutput> {
    ActorGuard.addGuard(action.url, new PollingDiffGuard(action, this));
    return {};
  }
}

export interface IActorGuardPollingArgs extends IActorGuardArgs {
  /**
   * The HTTP mediator
   */
  mediatorHttp: MediatorHttp;
  /**
   * The Dereference mediator
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
  /**
   * The Polling Frequency in seconds
   */
  pollingFrequency: number;
}
