import type {
  IActionQuerySourceIdentify,
  IActorQuerySourceIdentifyOutput,
  IActorQuerySourceIdentifyArgs,
  MediatorQuerySourceIdentify,
} from '@comunica/bus-query-source-identify';
import { ActorQuerySourceIdentify } from '@comunica/bus-query-source-identify';
import type { MediatorRdfMetadataAccumulate } from '@comunica/bus-rdf-metadata-accumulate';
import { KeysInitQuery } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { failTest, passTestVoid, ActionContext } from '@comunica/core';
import type { ComunicaDataFactory } from '@comunica/types';
import { StreamQuerySources } from './StreamQuerySources';

/**
 * An incremunica Stream Sources Query Source Identify Actor.
 */
export class ActorQuerySourceIdentifyStream extends ActorQuerySourceIdentify {
  public readonly mediatorQuerySourceIdentify: MediatorQuerySourceIdentify;
  public readonly mediatorRdfMetadataAccumulate: MediatorRdfMetadataAccumulate;

  public constructor(args: IActorQuerySourceIdentifyStreamSourcesArgs) {
    super(args);
  }

  public async test(action: IActionQuerySourceIdentify): Promise<TestResult<IActorTest>> {
    const source = action.querySourceUnidentified;
    if (source.type !== 'stream') {
      return failTest(`${this.name} requires a single query source with stream type to be present in the context.`);
    }
    return passTestVoid();
  }

  public async run(action: IActionQuerySourceIdentify): Promise<IActorQuerySourceIdentifyOutput> {
    const dataFactory: ComunicaDataFactory = action.context.getSafe(KeysInitQuery.dataFactory);
    return {
      querySource: {
        source: new StreamQuerySources(
          <any>action.querySourceUnidentified.value,
          dataFactory,
          this.mediatorQuerySourceIdentify,
          this.mediatorRdfMetadataAccumulate,
          action.context,
        ),
        context: action.querySourceUnidentified.context ?? new ActionContext(),
      },
    };
  }
}

export interface IActorQuerySourceIdentifyStreamSourcesArgs extends IActorQuerySourceIdentifyArgs {
  /**
   * A mediator for identifying query sources.
   */
  mediatorQuerySourceIdentify: MediatorQuerySourceIdentify;
  /**
   * A mediator for identifying query sources.
   */
  mediatorRdfMetadataAccumulate: MediatorQuerySourceIdentify;
}
