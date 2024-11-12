import { EventEmitter } from 'events';
import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';
import { StreamingQuerySourceRdfJs } from '@incremunica/actor-query-source-identify-streaming-rdfjs';
import type { IActionGuard, IActorGuardOutput, IActorGuardArgs } from '@incremunica/bus-guard';
import { ActorGuard } from '@incremunica/bus-guard';
import type { MediatorResourceWatch } from '@incremunica/bus-resource-watch';
import type { IGuardEvents, Quad } from '@incremunica/incremental-types';
import { StreamingQuerySourceStatus } from '@incremunica/streaming-query-source';

/**
 * A comunica Naive Guard Actor.
 */
export class ActorGuardNaive extends ActorGuard {
  public readonly mediatorResourceWatch: MediatorResourceWatch;
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;

  public constructor(args: IActorGuardNaiveArgs) {
    super(args);
  }

  public async test(action: IActionGuard): Promise<TestResult<IActorTest>> {
    if (
      action.streamingQuerySource === undefined ||
      !(action.streamingQuerySource instanceof StreamingQuerySourceRdfJs)
    ) {
      failTest('This actor only works on StreamingQuerySourceRdfJs');
    }
    return passTestVoid();
  }

  public async run(action: IActionGuard): Promise<IActorGuardOutput> {
    const resourceWatch = await this.mediatorResourceWatch.mediate({
      context: action.context,
      url: action.url,
      metadata: action.metadata,
    });
    const store = (<StreamingQuerySourceRdfJs>action.streamingQuerySource).store;

    if (action.streamingQuerySource.status === StreamingQuerySourceStatus.Running) {
      resourceWatch.start();
    }
    action.streamingQuerySource.statusEvents.on('status', (status: StreamingQuerySourceStatus) => {
      if (status === StreamingQuerySourceStatus.Running) {
        resourceWatch.start();
      } else {
        resourceWatch.stop();
      }
    });

    const guardEvents: IGuardEvents = new EventEmitter();
    guardEvents.emit('up-to-date');

    resourceWatch.events.on('update', () => {
      guardEvents.emit('modified');
      const deletionStore = store.copyOfStore();
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
            store.removeQuad(<Quad>quad);
          }
          for (const quad of additionArray) {
            store.addQuad(quad);
          }
          guardEvents.emit('up-to-date');
        });
      }).catch(() => {
        for (const quad of deletionStore) {
          store.removeQuad(<Quad>quad);
        }
        guardEvents.emit('up-to-date');
      });
    });

    resourceWatch.events.on('delete', () => {
      guardEvents.emit('modified');
      for (const quad of store.getStore()) {
        store.removeQuad(<Quad>quad);
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
