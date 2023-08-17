import type {
  IActionRdfResolveQuadPattern,
  IActorRdfResolveQuadPatternArgs,
  IQuadSource,
} from '@comunica/bus-rdf-resolve-quad-pattern';
import {
  ActorRdfResolveQuadPatternSource, getContextSource, hasContextSingleSourceOfType,
} from '@comunica/bus-rdf-resolve-quad-pattern';
import type { IActorTest } from '@comunica/core';
import type { IActionContext } from '@comunica/types';
import { RdfJsQuadStreamingSource } from './RdfJsQuadStreamingSource';

/**
 * A comunica RDFjs Streaming Source RDF Resolve Quad Pattern Actor.
 */
export class ActorRdfResolveQuadPatternRdfjsStreamingSource extends ActorRdfResolveQuadPatternSource {
  public constructor(args: IActorRdfResolveQuadPatternArgs) {
    super(args);
  }

  public async test(action: IActionRdfResolveQuadPattern): Promise<IActorTest> {
    if (!hasContextSingleSourceOfType('rdfjsSource', action.context)) {
      throw new Error(`${this.name} requires a single source with an rdfjsSource to be present in the context.`);
    }
    const source = getContextSource(action.context);
    if (!source || typeof source === 'string' || (!('match' in source) && !source.value.match)) {
      throw new Error(`${this.name} received an invalid rdfjsSource.`);
    }
    return true;
  }

  protected async getSource(context: IActionContext): Promise<IQuadSource> {
    const source: any = <any> getContextSource(context);
    return new RdfJsQuadStreamingSource('match' in source ? source : source.value, context);
  }
}
