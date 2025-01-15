import { EventEmitter } from 'events';
import type { MediatorDereferenceRdf } from '@comunica/bus-dereference-rdf';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid } from '@comunica/core';
import { StreamingQuerySourceRdfJs } from '@incremunica/actor-query-source-identify-streaming-rdfjs';
import type {
  IActionDetermineChanges,
  IActorDetermineChangesOutput,
  IActorDetermineChangesArgs,
} from '@incremunica/bus-determine-changes';
import { ActorDetermineChanges } from '@incremunica/bus-determine-changes';
import type { MediatorSourceWatch } from '@incremunica/bus-source-watch';
import { StreamingQuerySourceStatus } from '@incremunica/streaming-query-source';
import type { IDetermineChangesEvents, Quad } from '@incremunica/types';

/**
 * A comunica Naive Determine Changes Actor.
 */
export class ActorDetermineChangesNaive extends ActorDetermineChanges {
  public readonly mediatorSourceWatch: MediatorSourceWatch;
  public readonly mediatorDereferenceRdf: MediatorDereferenceRdf;

  public constructor(args: IActorDetermineChangesNaiveArgs) {
    super(args);
  }

  public async test(action: IActionDetermineChanges): Promise<TestResult<IActorTest>> {
    if (
      action.streamingQuerySource === undefined ||
      !(action.streamingQuerySource instanceof StreamingQuerySourceRdfJs)
    ) {
      failTest('This actor only works on StreamingQuerySourceRdfJs');
    }
    return passTestVoid();
  }

  public async run(action: IActionDetermineChanges): Promise<IActorDetermineChangesOutput> {
    const sourceWatch = await this.mediatorSourceWatch.mediate({
      context: action.context,
      url: action.url,
      metadata: action.metadata,
    });
    const store = (<StreamingQuerySourceRdfJs>action.streamingQuerySource).store;

    if (action.streamingQuerySource.status === StreamingQuerySourceStatus.Running) {
      sourceWatch.start();
    }
    action.streamingQuerySource.statusEvents.on('status', (status: StreamingQuerySourceStatus) => {
      if (status === StreamingQuerySourceStatus.Running) {
        sourceWatch.start();
      } else {
        sourceWatch.stop();
      }
    });

    const determineChangesEvents: IDetermineChangesEvents = new EventEmitter();
    determineChangesEvents.emit('up-to-date');

    sourceWatch.events.on('update', () => {
      determineChangesEvents.emit('modified');
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
          determineChangesEvents.emit('up-to-date');
        });
      }).catch(() => {
        for (const quad of deletionStore) {
          store.removeQuad(<Quad>quad);
        }
        determineChangesEvents.emit('up-to-date');
      });
    });

    sourceWatch.events.on('delete', () => {
      determineChangesEvents.emit('modified');
      for (const quad of store.getStore()) {
        store.removeQuad(<Quad>quad);
      }
      determineChangesEvents.emit('up-to-date');
    });

    return { determineChangesEvents };
  }
}

export interface IActorDetermineChangesNaiveArgs extends IActorDetermineChangesArgs {
  /**
   * The Source Watch mediator
   */
  mediatorSourceWatch: MediatorSourceWatch;
  /**
   * The Dereference mediator
   */
  mediatorDereferenceRdf: MediatorDereferenceRdf;
}
