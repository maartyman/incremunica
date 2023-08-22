import { EventEmitter } from 'events';
import type { Quad } from '@comunica/incremental-types';
import type * as RDF from '@rdfjs/types';
import { Store } from 'n3';
import { Readable, PassThrough } from 'readable-stream';
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
export class StreamingStore<Q extends Quad, S extends RDF.Store<Q> = Store<Q>>
  extends EventEmitter implements RDF.Source<Q>, RDF.Sink<RDF.Stream<Q>, EventEmitter> {
  protected readonly store: S;
  protected readonly pendingStreams: PendingStreamsIndex<Q> = new PendingStreamsIndex();
  protected ended = false;
  protected numberOfListeners = 0;
  protected halted = false;
  protected haltBuffer = new Array<Q>();

  public constructor(store: RDF.Store<Q> = new Store<Q>()) {
    super();
    this.store = <S> store;
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
    for (const pendingStream of this.pendingStreams.allStreams) {
      pendingStream.push(null);
      (<any> pendingStream)._pipeSource.unpipe();
    }

    this.emit('end');
  }

  public hasEnded(): boolean {
    return this.ended;
  }

  public halt(): void {
    this.halted = true;
  }

  public resume(): void {
    for (const quad of this.haltBuffer) {
      for (const pendingStream of this.pendingStreams.getPendingStreamsForQuad(quad)) {
        if (!this.ended) {
          pendingStream.push(quad);
        }
      }
      if (quad.diff) {
        this.store.import(new Readable({
          read(size: number) {
            this.push(quad);
            this.destroy();
          },
          objectMode: true,
        }));
      } else {
        this.store.removeMatches(quad.subject, quad.predicate, quad.object, quad.graph);
      }
    }
    this.halted = false;
  }

  public isHalted(): boolean {
    return this.halted;
  }

  public copyOfStore(): Store {
    const newStore = new Store();
    this.store.match().on('data', quad => {
      newStore.add(quad);
    });
    return newStore;
  }

  public remove(stream: RDF.Stream<Q>): EventEmitter {
    if (this.ended) {
      throw new Error('Attempted to remove out of an ended StreamingStore');
    }

    stream.on('data', (quad: Q) => {
      if (quad.diff === undefined) {
        quad.diff = false;
      }
      if (this.halted) {
        this.haltBuffer.push(quad);
        return;
      }
      for (const pendingStream of this.pendingStreams.getPendingStreamsForQuad(quad)) {
        if (!this.ended) {
          pendingStream.push(quad);
        }
      }
      if (quad.diff) {
        this.store.import(new Readable({
          read(size: number) {
            this.push(quad);
            this.destroy();
          },
          objectMode: true,
        }));
      } else {
        this.store.removeMatches(quad.subject, quad.predicate, quad.object, quad.graph);
      }
    });

    return stream;
  }

  public import(stream: RDF.Stream<Q>): EventEmitter {
    if (this.ended) {
      throw new Error('Attempted to import into an ended StreamingStore');
    }

    stream.on('data', (quad: Q) => {
      if (quad.diff === undefined) {
        quad.diff = true;
      }
      if (this.halted) {
        this.haltBuffer.push(quad);
        return;
      }
      for (const pendingStream of this.pendingStreams.getPendingStreamsForQuad(quad)) {
        if (!this.ended) {
          pendingStream.push(quad);
        }
      }
      if (quad.diff) {
        this.store.import(new Readable({
          read(size: number) {
            this.push(quad);
            this.destroy();
          },
          objectMode: true,
        }));
      } else {
        this.store.removeMatches(quad.subject, quad.predicate, quad.object, quad.graph);
      }
    });

    return stream;
  }

  public addQuad(quad: Q): this {
    if (this.ended) {
      throw new Error('Attempted to add a quad into an ended StreamingStore.');
    }
    if (quad.diff === undefined) {
      quad.diff = true;
    }
    if (this.halted) {
      this.haltBuffer.push(quad);
      return this;
    }
    for (const pendingStream of this.pendingStreams.getPendingStreamsForQuad(quad)) {
      if (!this.ended) {
        pendingStream.push(quad);
      }
    }
    if (quad.diff) {
      this.store.import(new Readable({
        read(size: number) {
          this.push(quad);
          this.destroy();
        },
        objectMode: true,
      }));
    } else {
      this.store.removeMatches(quad.subject, quad.predicate, quad.object, quad.graph);
    }
    return this;
  }

  public removeQuad(quad: Q): this {
    if (this.ended) {
      throw new Error('Attempted to remove a quad of an ended StreamingStore.');
    }
    if (quad.diff === undefined) {
      quad.diff = false;
    }
    if (this.halted) {
      this.haltBuffer.push(quad);
      return this;
    }
    for (const pendingStream of this.pendingStreams.getPendingStreamsForQuad(quad)) {
      if (!this.ended) {
        pendingStream.push(quad);
      }
    }
    if (quad.diff) {
      this.store.import(new Readable({
        read(size: number) {
          this.push(quad);
          this.destroy();
        },
        objectMode: true,
      }));
    } else {
      this.store.removeMatches(quad.subject, quad.predicate, quad.object, quad.graph);
    }
    return this;
  }

  public match(
    subject?: RDF.Term | null,
    predicate?: RDF.Term | null,
    object?: RDF.Term | null,
    graph?: RDF.Term | null,
  ): RDF.Stream<Q> {
    // TODO what if match is never called (=> streaming store should be removed) (Should not happen I think)
    this.numberOfListeners++;
    const storeResult: Readable = <Readable> this.store.match(subject, predicate, object, graph);
    let stream: RDF.Stream<Q> = storeResult;

    // If the store hasn't ended yet, also create a new pendingStream
    if (!this.ended) {
      // The new pendingStream remains open, until the store is ended.
      const pendingStream = new PassThrough({ objectMode: true });
      this.pendingStreams.addPatternListener(pendingStream, subject, predicate, object, graph);
      stream = storeResult.pipe(pendingStream, { end: false });
      (<any> stream)._pipeSource = storeResult;
    }

    stream.on('close', () => {
      if (this.numberOfListeners < 2) {
        this.end();
      } else {
        this.numberOfListeners--;
      }
    });

    return stream;
  }

  /**
   * The internal store with all imported quads.
   */
  public getStore(): S {
    return this.store;
  }
}
