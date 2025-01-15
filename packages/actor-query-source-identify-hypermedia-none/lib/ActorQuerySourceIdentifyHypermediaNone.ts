import type { MediatorMergeBindingsContext } from '@comunica/bus-merge-bindings-context';
import type {
  IActionQuerySourceIdentifyHypermedia,
  IActorQuerySourceIdentifyHypermediaOutput,
  IActorQuerySourceIdentifyHypermediaArgs,
  IActorQuerySourceIdentifyHypermediaTest,
} from '@comunica/bus-query-source-identify-hypermedia';
import { ActorQuerySourceIdentifyHypermedia } from '@comunica/bus-query-source-identify-hypermedia';
import { KeysInitQuery } from '@comunica/context-entries';
import type { TestResult } from '@comunica/core';
import { ActionContext, passTest } from '@comunica/core';
import type { ComunicaDataFactory } from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { StreamingQuerySourceRdfJs } from '@incremunica/actor-query-source-identify-streaming-rdfjs';
import type { MediatorDetermineChanges } from '@incremunica/bus-determine-changes';
import { KeysDetermineChanges } from '@incremunica/context-entries';
import type { Quad } from '@incremunica/incremental-types';
import { StreamingStore } from '@incremunica/streaming-store';
import type * as RDF from '@rdfjs/types';

/**
 * An incremunica Stream None Query Source Identify Hypermedia Actor.
 */
export class ActorQuerySourceIdentifyHypermediaNone extends ActorQuerySourceIdentifyHypermedia {
  public readonly mediatorMergeBindingsContext: MediatorMergeBindingsContext;
  public readonly mediatorDetermineChanges: MediatorDetermineChanges;

  public constructor(args: IActorQuerySourceIdentifyHypermediaNoneArgs) {
    super(args, 'file');
  }

  public async testMetadata(
    _action: IActionQuerySourceIdentifyHypermedia,
  ): Promise<TestResult<IActorQuerySourceIdentifyHypermediaTest>> {
    return passTest({ filterFactor: 0 });
  }

  public async run(action: IActionQuerySourceIdentifyHypermedia): Promise<IActorQuerySourceIdentifyHypermediaOutput> {
    this.logInfo(action.context, `Identified as file source: ${action.url}`);
    const store = new StreamingStore<Quad>();
    store.import(<RDF.Stream<Quad>><any>action.quads);
    const dataFactory: ComunicaDataFactory = action.context.getSafe(KeysInitQuery.dataFactory);
    const source = new StreamingQuerySourceRdfJs(
      store,
      dataFactory,
      await BindingsFactory.create(this.mediatorMergeBindingsContext, action.context, dataFactory),
    );
    source.toString = () => `ActorQuerySourceIdentifyHypermediaNone(${action.url})`;
    source.referenceValue = action.url;

    const { determineChangesEvents } = await this.mediatorDetermineChanges.mediate({
      context: action.context,
      url: action.url,
      metadata: action.metadata,
      streamingQuerySource: source,
    });

    if (source.context) {
      source.context = source.context.set(KeysDetermineChanges.events, determineChangesEvents);
    } else {
      source.context = new ActionContext().set(KeysDetermineChanges.events, determineChangesEvents);
    }

    return { source };
  }
}

export interface IActorQuerySourceIdentifyHypermediaNoneArgs extends IActorQuerySourceIdentifyHypermediaArgs {
  /**
   * The Determine Changes mediator
   */
  mediatorDetermineChanges: MediatorDetermineChanges;
  /**
   * A mediator for creating binding context merge handlers
   */
  mediatorMergeBindingsContext: MediatorMergeBindingsContext;
}
