import {
  ActorRdfResolveHypermedia,
  IActionRdfResolveHypermedia,
  IActorRdfResolveHypermediaOutput,
  IActorRdfResolveHypermediaArgs,
  IActorRdfResolveHypermediaTest
} from '@comunica/bus-rdf-resolve-hypermedia';
import {RdfJsQuadSource} from "@comunica/actor-rdf-resolve-quad-pattern-rdfjs-source";
import {storeStream} from "rdf-store-stream";
import {RdfJsQuadStreamSource} from "./RdfJsQuadStreamSource";

/**
 * A comunica Stream Source RDF Resolve Hypermedia Actor.
 */
export class ActorRdfResolveHypermediaStreamSource extends ActorRdfResolveHypermedia {
  public constructor(args: IActorRdfResolveHypermediaArgs) {
    super(args, 'file');
  }

  public async testMetadata(action: IActionRdfResolveHypermedia): Promise<IActorRdfResolveHypermediaTest> {
    return { filterFactor: 0 };
  }

  public async run(action: IActionRdfResolveHypermedia): Promise<IActorRdfResolveHypermediaOutput> {
    this.logInfo(action.context, `Identified as file source: ${action.url}`);
    return { source: new RdfJsQuadStreamSource(action) };
  }
}
