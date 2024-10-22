import type { MediatorMergeBindingsContext } from '@comunica/bus-merge-bindings-context';
import type {
  IActionQuerySourceIdentify,
  IActorQuerySourceIdentifyOutput,
  IActorQuerySourceIdentifyArgs,
} from '@comunica/bus-query-source-identify';
import { ActorQuerySourceIdentify } from '@comunica/bus-query-source-identify';
import { KeysInitQuery } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid, ActionContext } from '@comunica/core';
import type { ComunicaDataFactory } from '@comunica/types';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import type { StreamingStore } from '@incremunica/incremental-rdf-streaming-store';
import type { Quad } from '@incremunica/incremental-types';
import { StreamingQuerySourceRdfJs } from './StreamingQuerySourceRdfJs';

/**
 * An incremunica Streaming RDFJS Query Source Identify Actor.
 */
export class ActorQuerySourceIdentifyStreamingRdfJs extends ActorQuerySourceIdentify {
  public readonly mediatorMergeBindingsContext: MediatorMergeBindingsContext;

  public constructor(args: IActorQuerySourceIdentifyStreamingRdfJsArgs) {
    super(args);
  }

  public async test(action: IActionQuerySourceIdentify): Promise<TestResult<IActorTest>> {
    const source = action.querySourceUnidentified;
    if (source.type !== undefined && source.type !== 'rdfjs') {
      return fail(`${this.name} requires a single query source with rdfjs type to be present in the context.`);
    }
    if (typeof source.value === 'string' || !('match' in source.value)) {
      return fail(`${this.name} actor received an invalid streaming rdfjs query source.`);
    }
    // TODO add check to make sure the store is a streaming store
    // if (!(source.value instanceof StreamingStore)
    // && !(!('match' in source) && (source.value instanceof StreamingStore))) {
    //  throw new Error(`${this.name} didn't receive a StreamingStore.`);
    // }
    return passTestVoid();
  }

  public async run(action: IActionQuerySourceIdentify): Promise<IActorQuerySourceIdentifyOutput> {
    const dataFactory: ComunicaDataFactory = action.context.getSafe(KeysInitQuery.dataFactory);
    return {
      querySource: {
        source: new StreamingQuerySourceRdfJs(
          <StreamingStore<Quad>>action.querySourceUnidentified.value,
          dataFactory,
          await BindingsFactory.create(this.mediatorMergeBindingsContext, action.context, dataFactory),
        ),
        context: action.querySourceUnidentified.context ?? new ActionContext(),
      },
    };
  }
}

export interface IActorQuerySourceIdentifyStreamingRdfJsArgs extends IActorQuerySourceIdentifyArgs {
  /**
   * A mediator for creating binding context merge handlers
   */
  mediatorMergeBindingsContext: MediatorMergeBindingsContext;
}
