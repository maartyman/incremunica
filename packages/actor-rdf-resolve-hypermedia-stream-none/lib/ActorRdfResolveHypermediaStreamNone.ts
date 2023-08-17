import { RdfJsQuadStreamingSource } from '@comunica/actor-rdf-resolve-quad-pattern-rdfjs-streaming-source';
import type { MediatorGuard } from '@comunica/bus-guard';
import type {
  IActionRdfResolveHypermedia,
  IActorRdfResolveHypermediaOutput,
  IActorRdfResolveHypermediaArgs,
  IActorRdfResolveHypermediaTest,
} from '@comunica/bus-rdf-resolve-hypermedia';
import {
  ActorRdfResolveHypermedia,
} from '@comunica/bus-rdf-resolve-hypermedia';
import { StreamingStore } from '@comunica/incremental-rdf-streaming-store';

/**
 * A comunica Stream None RDF Resolve Hypermedia Actor.
 */
export class ActorRdfResolveHypermediaStreamNone extends ActorRdfResolveHypermedia {
  public readonly mediatorGuard: MediatorGuard;

  public constructor(args: IActorRdfResolveHypermediaStreamSourceArgs) {
    super(args, 'file');
  }

  public async testMetadata(action: IActionRdfResolveHypermedia): Promise<IActorRdfResolveHypermediaTest> {
    return { filterFactor: 0 };
  }

  public async run(action: IActionRdfResolveHypermedia): Promise<IActorRdfResolveHypermediaOutput> {
    this.logInfo(action.context, `Identified as file source: ${action.url}`);
    const store = new StreamingStore();
    store.import(<any>action.quads);
    const source = new RdfJsQuadStreamingSource(store);

    await this.mediatorGuard.mediate({
      context: action.context,
      url: action.url,
      metadata: action.metadata,
      streamingSource: source,
    });

    return { source };
  }
}

export interface IActorRdfResolveHypermediaStreamSourceArgs extends IActorRdfResolveHypermediaArgs {
  /**
   * The Guard mediator
   */
  mediatorGuard: MediatorGuard;
}
