import { EventEmitter } from 'events';
import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type { IActionGuard, IActorGuardOutput, IActorGuardArgs } from '@incremunica/bus-guard';
import { ActorGuard } from '@incremunica/bus-guard';
import type { MediatorResourceWatch } from '@incremunica/bus-resource-watch';
import type { IGuardEvents, Quad } from '@incremunica/incremental-types';

/**
 * A comunica Naive Guard Actor.
 */
export class ActorGuardNaive extends ActorGuard {
  public readonly mediatorResourceWatch: MediatorResourceWatch;
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;

  public constructor(args: IActorGuardNaiveArgs) {
    super(args);
  }

  public async test(_action: IActionGuard): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async run(action: IActionGuard): Promise<IActorGuardOutput> {
    const resourceWatch = await this.mediatorResourceWatch.mediate({
      context: action.context,
      url: action.url,
      metadata: action.metadata,
    });

    const guardEvents: IGuardEvents = new EventEmitter();
    guardEvents.emit('up-to-date');

    // If the streamingStore has ended while making a watcher, stop watching
    if (action.streamingSource.store.hasEnded()) {
      resourceWatch.stopFunction();
      return { guardEvents };
    }
    action.streamingSource.store.on('end', () => {
      resourceWatch.stopFunction();
    });

    resourceWatch.events.on('update', () => {
      guardEvents.emit('modified');
      const deletionStore = action.streamingSource.store.copyOfStore();
      const additionArray: Quad[] = [];
      this.mediatorDereferenceRdf.mediate({
        context: action.context,
        url: action.url,
      }).then((responseGet) => {
        responseGet.data.on('data', (quad) => {
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
          guardEvents.emit('up-to-date');
        });
      }).catch(() => {
        for (const quad of deletionStore) {
          action.streamingSource.store.removeQuad(<Quad>quad);
        }
        guardEvents.emit('up-to-date');
      });
    });

    resourceWatch.events.on('delete', () => {
      guardEvents.emit('modified');
      for (const quad of action.streamingSource.store.getStore()) {
        action.streamingSource.store.removeQuad(<Quad>quad);
      }
      guardEvents.emit('up-to-date');
    });

    return { guardEvents };
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
