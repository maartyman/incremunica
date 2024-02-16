import { EventEmitter } from 'events';
import type { Quad } from '@incremunica/incremental-types';
import type * as RDF from '@rdfjs/types';
import type { Term } from 'n3';
import { Store } from 'n3';
import { Readable, PassThrough } from 'readable-stream';
import { LinkedList } from './LinkedList';
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
  protected numberOfListeners = 0;
  protected halted = false;
  protected haltBuffer = new LinkedList<Q>();

  public constructor(store = new Store<Q>()) {
    super();
    this.store = store;
    this.setMaxListeners(Number.POSITIVE_INFINITY);
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
    if ((quad.diff && this.store.has(quad)) || (!quad.diff && !this.store.has(quad))) {
      return;
    }
    for (const pendingStream of this.pendingStreams.getPendingStreamsForQuad(quad)) {
      if (!this.ended) {
        pendingStream.push(quad);
      }
    }
    if (quad.diff) {
      this.store.add(quad);
    } else {
      this.store.delete(quad);
    }
  }

  public halt(): void {
    if (!this.halted) {
      this.emit('halt');
    }
    this.halted = true;
  }

  public flush(): void {
    let quad = this.haltBuffer.shift();
    while (quad) {
      this.handleQuad(quad);
      quad = this.haltBuffer.shift();
    }
    for (const pendingStreams of this.pendingStreams.indexedStreams.values()) {
      for (const pendingStream of pendingStreams) {
        if (!this.ended) {
          pendingStream.push('pause');
        }
      }
    }
    this.emit('flush');
  }

  public resume(): void {
    if (this.halted) {
      let quad = this.haltBuffer.shift();
      while (quad) {
        this.handleQuad(quad);
        quad = this.haltBuffer.shift();
      }
      for (const pendingStreams of this.pendingStreams.indexedStreams.values()) {
        for (const pendingStream of pendingStreams) {
          if (!this.ended) {
            pendingStream.push('pause');
          }
        }
      }
      this.halted = false;
      this.emit('resume');
    }
  }

  public isHalted(): boolean {
    return this.halted;
  }

  public copyOfStore(): Store {
    const newStore = new Store<Quad>();
    this.store.forEach(quad => {
      newStore.add(quad);
    }, null, null, null, null);
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
      if (quad.diff === undefined) {
        quad.diff = true;
      }
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
    if (quad.diff === undefined) {
      quad.diff = true;
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
    if (quad.diff === undefined) {
      quad.diff = false;
    }
    if (this.halted) {
      this.haltBuffer.push(quad);
    } else {
      this.handleQuad(quad);
    }
    return this;
  }

  public match(
    subject?: RDF.Term | null,
    predicate?: RDF.Term | null,
    object?: RDF.Term | null,
    graph?: RDF.Term | null,
    options?: { stopMatch: () => void },
  ): RDF.Stream<Q> {
    // TODO what if match is never called (=> streaming store should be removed) (Should not happen I think)
    this.numberOfListeners++;
    const unionStream = new PassThrough({ objectMode: true });

    const storedQuads = this.store.getQuads(
      <Term>subject,
      <Term>predicate,
      <Term>object,
      <Term>graph,
    );
    const storeResult = new Readable({
      objectMode: true,
      read() {
        if (storedQuads.length > 0) {
          storeResult.push(storedQuads.pop());
        }
        if (storedQuads.length === 0) {
          storeResult.push(null);
        }
      },
    });
    storeResult.pipe(unionStream, { end: false });

    // If the store hasn't ended yet, also create a new pendingStream
    if (!this.ended) {
      // The new pendingStream remains open, until the store is ended.
      const pendingStream = new PassThrough({ objectMode: true });
      if (options) {
        options.stopMatch = () => {
          pendingStream.end();
          this.pendingStreams.removeClosedPatternListener(subject, predicate, object, graph);
        };
      }
      this.pendingStreams.addPatternListener(pendingStream, subject, predicate, object, graph);
      pendingStream.pipe(unionStream, { end: false });
      pendingStream.pause();

      let pendingStreamEnded = false;
      let storeResultEnded = false;

      pendingStream.on('close', () => {
        pendingStreamEnded = true;
        if (storeResultEnded) {
          unionStream.end();
        }
      });

      storeResult.on('close', () => {
        storeResultEnded = true;
        if (pendingStreamEnded) {
          unionStream.end();
        } else {
          pendingStream.resume();
          if (this.halted) {
            pendingStream.push('pause');
          }
        }
      });
    } else {
      storeResult.on('close', () => {
        unionStream.end();
      });
    }

    unionStream.on('close', () => {
      if (this.numberOfListeners < 2) {
        this.end();
      } else {
        this.numberOfListeners--;
      }
    });

    return unionStream;
  }

  /**
   * The internal store with all imported quads.
   */
  public getStore(): Store {
    return this.store;
  }
}
