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
import type { MediatorGuard } from '@incremunica/bus-guard';
import { KeysGuard } from '@incremunica/context-entries';
import { StreamingStore } from '@incremunica/incremental-rdf-streaming-store';
import type { Quad } from '@incremunica/incremental-types';
import type * as RDF from '@rdfjs/types';

/**
 * An incremunica Stream None Query Source Identify Hypermedia Actor.
 */
export class ActorQuerySourceIdentifyHypermediaNone extends ActorQuerySourceIdentifyHypermedia {
  public readonly mediatorMergeBindingsContext: MediatorMergeBindingsContext;
  public readonly mediatorGuard: MediatorGuard;

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

    const { guardEvents } = await this.mediatorGuard.mediate({
      context: action.context,
      url: action.url,
      metadata: action.metadata,
      streamingQuerySource: source,
    });

    if (source.context) {
      source.context = source.context.set(KeysGuard.events, guardEvents);
    } else {
      source.context = new ActionContext().set(KeysGuard.events, guardEvents);
    }

    return { source };
  }
}

export interface IActorQuerySourceIdentifyHypermediaNoneArgs extends IActorQuerySourceIdentifyHypermediaArgs {
  /**
   * The Guard mediator
   */
  mediatorGuard: MediatorGuard;
  /**
   * A mediator for creating binding context merge handlers
   */
  mediatorMergeBindingsContext: MediatorMergeBindingsContext;
}
