import {
  ActorRdfResolveHypermedia,
  IActionRdfResolveHypermedia,
  IActorRdfResolveHypermediaOutput,
  IActorRdfResolveHypermediaArgs,
  IActorRdfResolveHypermediaTest
} from '@comunica/bus-rdf-resolve-hypermedia';
import {RdfJsQuadStreamSource} from "./RdfJsQuadStreamSource";
import {MediatorGuard} from "@comunica/bus-guard";

/**
 * A comunica Stream Source RDF Resolve Hypermedia Actor.
 */
export class ActorRdfResolveHypermediaStreamSource extends ActorRdfResolveHypermedia {
  public readonly mediatorGuard: MediatorGuard;

  public constructor(args: IActorRdfResolveHypermediaStreamSourceArgs) {
    super(args, 'file');
  }

  public async testMetadata(action: IActionRdfResolveHypermedia): Promise<IActorRdfResolveHypermediaTest> {
    return { filterFactor: 0 };
  }

  public async run(action: IActionRdfResolveHypermedia): Promise<IActorRdfResolveHypermediaOutput> {
    this.logInfo(action.context, `Identified as file source: ${action.url}`);
    let source = new RdfJsQuadStreamSource(action);

    await this.mediatorGuard.mediate({
      context: action.context,
      streamSource: source
    });

    return { source: source};
  }
}

export interface IActorRdfResolveHypermediaStreamSourceArgs extends IActorRdfResolveHypermediaArgs {
  /**
   * The Guard mediator
   */
  mediatorGuard: MediatorGuard;
}
