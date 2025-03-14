import { EventEmitter } from 'events';
import type { Quad } from '@incremunica/types';
import type * as RDF from '@rdfjs/types';
import type { Term } from 'n3';
import { Store } from 'n3';
import { PassThrough } from 'readable-stream';
import { PendingStreamsIndex } from './PendingStreamsIndex';

/**
 * A StreamingStore allows data lookup and insertion to happen in parallel.
 * Concretely, this means that `match()` calls happening before `import()` calls, will still consider those triples that
 * are inserted later, which is done by keeping the response streams of `match()` open.
 * Only when the `end()` method is invoked, all response streams will close, and the StreamingStore will be considered
 * immutable.
 *
 * WARNING: `end()` MUST be called at some point, otherwise all `match` streams will remain unended.
 */
export class StreamingStore<Q extends Quad>
  extends EventEmitter implements RDF.Source<Q>, RDF.Sink<RDF.Stream<Q>, EventEmitter> {
  protected readonly store: Store;
  protected readonly pendingStreams: PendingStreamsIndex<Q> = new PendingStreamsIndex();
  protected ended = false;
  protected halted = false;
  protected haltBuffer = new Array<Q>();

  public constructor(store = new Store<Q>()) {
    super();
    this.store = store;
  }

  /**
   * Mark this store as ended.
   *
   * This will make sure that all running and future `match` calls will end,
   * and all next `import` calls to this store will throw an error.
   */
  public end(): void {
    this.ended = true;

    // Mark all pendingStreams as ended.
    for (const pendingStreams of this.pendingStreams.indexedStreams.values()) {
      for (const pendingStream of pendingStreams) {
        pendingStream.end();
      }
    }

    this.emit('end');
  }

  public hasEnded(): boolean {
    return this.ended;
  }

  private handleQuad(quad: Q): void {
    quad.isAddition = quad.isAddition ?? true;
    if (quad.isAddition ? this.store.has(quad) : !this.store.has(quad)) {
      return;
    }
    for (const pendingStream of this.pendingStreams.getPendingStreamsForQuad(quad)) {
      if (!this.ended) {
        pendingStream.push(quad);
      }
    }
    if (quad.isAddition) {
      this.store.add(quad);
    } else {
      this.store.delete(quad);
    }
  }

  public halt(): void {
    this.halted = true;
  }

  public resume(): void {
    for (const quad of this.haltBuffer) {
      this.handleQuad(quad);
    }
    this.haltBuffer = new Array<Q>();
    this.halted = false;
  }

  public isHalted(): boolean {
    return this.halted;
  }

  public copyOfStore(): Store {
    const newStore = new Store<Quad>();
    for (const quad of this.store) {
      newStore.add(quad);
    }
    return newStore;
  }

  public remove(stream: RDF.Stream<Q>): EventEmitter {
    if (this.ended) {
      throw new Error('Attempted to remove out of an ended StreamingStore');
    }

    stream.on('data', (quad: Q) => {
      quad.isAddition = quad.isAddition ?? false;
      if (this.halted) {
        this.haltBuffer.push(quad);
      } else {
        this.handleQuad(quad);
      }
    });
    return stream;
  }

  public import(stream: RDF.Stream<Q>): EventEmitter {
    if (this.ended) {
      throw new Error('Attempted to import into an ended StreamingStore');
    }

    stream.on('data', (quad: Q) => {
      if (this.halted) {
        this.haltBuffer.push(quad);
      } else {
        this.handleQuad(quad);
      }
    });
    return stream;
  }

  public addQuad(quad: Q): this {
    if (this.ended) {
      throw new Error('Attempted to add a quad into an ended StreamingStore.');
    }
    if (this.halted) {
      this.haltBuffer.push(quad);
    } else {
      this.handleQuad(quad);
    }
    return this;
  }

  public removeQuad(quad: Q): this {
    if (this.ended) {
      throw new Error('Attempted to remove a quad of an ended StreamingStore.');
    }
    quad.isAddition = quad.isAddition ?? false;
    if (this.halted) {
      this.haltBuffer.push(quad);
    } else {
      this.handleQuad(quad);
    }
    return this;
  }

  /**
   * Return a stream of quads matching the given pattern.
   *
   * @param subject               An optional subject.
   * @param predicate             An optional predicate.
   * @param object                An optional object.
   * @param graph                 An optional graph.
   * @param options               An optional object that will override 2 functions that control the output stream.
   * @param options.closeStream   A function that ends the stream.
   * @param options.deleteStream  A function that first propagates the results as deletions and then end the stream.
   */
  public match(
    subject: RDF.Term | null,
    predicate: RDF.Term | null,
    object: RDF.Term | null,
    graph: RDF.Term | null,
    options?: { closeStream?: () => void; deleteStream?: () => void },
  ): RDF.Stream<Q> {
    const unionStream = new PassThrough({ objectMode: true });

    const storedQuads: Quad[] = this.store.getQuads(
      subject,
      predicate,
      object,
      graph,
    );
    for (const quad of storedQuads) {
      quad.isAddition = true;
      unionStream.push(quad);
    }

    // If the store hasn't ended yet, also create a new pendingStream
    if (this.ended) {
      unionStream.end();
    } else {
      // The new pendingStream remains open, until the store is ended.
      const pendingStream = new PassThrough({ objectMode: true });
      if (options) {
        options.closeStream = () => {
          this.pendingStreams.removeClosedPatternListener(subject, predicate, object, graph);
          pendingStream.end();
        };
        options.deleteStream = () => {
          this.pendingStreams.removeClosedPatternListener(subject, predicate, object, graph);
          for (const quad of this.store.getQuads(
            <Term>subject,
            <Term>predicate,
            <Term>object,
            <Term>graph,
          )) {
            (<Quad>quad).isAddition = false;
            pendingStream.push(quad);
          }
          pendingStream.end();
        };
      }
      this.pendingStreams.addPatternListener(pendingStream, subject, predicate, object, graph);
      pendingStream.pipe(unionStream);
    }
    return unionStream;
  }

  /**
   * Return the number of quads matching the given pattern.
   *
   * @param subject   An optional subject.
   * @param predicate An optional predicate.
   * @param object    An optional object.
   * @param graph     An optional graph.
   */
  public countQuads(
    subject: RDF.Term | null,
    predicate: RDF.Term | null,
    object: RDF.Term | null,
    graph: RDF.Term | null,
  ): number {
    return this.store.countQuads(subject, predicate, object, graph);
  }

  /**
   * The internal store with all imported quads.
   */
  public getStore(): Store {
    return this.store;
  }
}
