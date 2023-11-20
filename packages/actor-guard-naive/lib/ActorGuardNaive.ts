import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type { IActorTest } from '@comunica/core';
import type { IActionGuard, IActorGuardOutput, IActorGuardArgs } from '@incremunica/bus-guard';
import { ActorGuard } from '@incremunica/bus-guard';
import type { MediatorResourceWatch } from '@incremunica/bus-resource-watch';
import type { Quad } from '@incremunica/incremental-types';

/**
 * A comunica Naive Guard Actor.
 */
export class ActorGuardNaive extends ActorGuard {
  public readonly mediatorResourceWatch: MediatorResourceWatch;
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;

  public constructor(args: IActorGuardNaiveArgs) {
    super(args);
  }

  public async test(action: IActionGuard): Promise<IActorTest> {
    return true;
  }

  public async run(action: IActionGuard): Promise<IActorGuardOutput> {
    const resourceWatch = await this.mediatorResourceWatch.mediate({
      context: action.context,
      url: action.url,
      metadata: action.metadata,
    });

    // If the streamingStore has ended while making a watcher, stop watching
    if (action.streamingSource.store.hasEnded()) {
      resourceWatch.stopFunction();
      return {};
    }
    action.streamingSource.store.on('end', () => {
      resourceWatch.stopFunction();
    });

    resourceWatch.events.on('update', async() => {
      const deletionStore = action.streamingSource.store.copyOfStore();
      const additionArray: Quad[] = [];
      const responseGet = await this.mediatorDereferenceRdf.mediate({
        context: action.context,
        url: action.url,
      });

      responseGet.data.on('data', quad => {
        if (deletionStore.has(quad)) {
          deletionStore.delete(quad);
          return;
        }
        additionArray.push(quad);
      });

      responseGet.data.on('end', () => {
        for (const quad of deletionStore) {
          action.streamingSource.store.removeQuad(<Quad>quad);
        }
        for (const quad of additionArray) {
          action.streamingSource.store.addQuad(quad);
        }
      });
    });

    resourceWatch.events.on('delete', () => {
      for (const quad of action.streamingSource.store.getStore()) {
        action.streamingSource.store.removeQuad(<Quad>quad);
      }
    });

    return {};
  }
}

export interface IActorGuardNaiveArgs extends IActorGuardArgs {
  /**
   * The Resource Watch mediator
   */
  mediatorResourceWatch: MediatorResourceWatch;
  /**
   * The Dereference mediator
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
}
