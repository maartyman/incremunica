import type { Bindings, BindingsStream } from '@incremunica/incremental-types';
import { AsyncIterator } from 'asynciterator';

export abstract class IncrementalInnerJoin extends AsyncIterator<Bindings> {
  protected leftIterator: BindingsStream;
  protected rightIterator: BindingsStream;
  protected funJoin: (...bindings: Bindings[]) => Bindings | null;

  public constructor(
    left: BindingsStream,
    right: BindingsStream,
    funJoin: (...bindings: Bindings[]) => Bindings | null,
  ) {
    super();
    this.leftIterator = left;
    this.rightIterator = right;
    this.funJoin = funJoin;

    this.on('end', () => this._cleanup());

    if (this.leftIterator.readable || this.rightIterator.readable) {
      this.readable = true;
    }

    this.leftIterator.on('error', error => this.destroy(error));
    this.rightIterator.on('error', error => this.destroy(error));

    this.leftIterator.on('readable', () => {
      this.readable = true;
    });
    this.rightIterator.on('readable', () => {
      this.readable = true;
    });

    this.leftIterator.on('end', () => {
      if (!this.hasResults()) {
        this._end();
      }
    });
    this.rightIterator.on('end', () => {
      if (!this.hasResults()) {
        this._end();
      }
    });
  }

  protected abstract _cleanup(): void;

  protected abstract hasResults(): boolean;

  public _end(): void {
    super._end();
    this.leftIterator.destroy();
    this.rightIterator.destroy();
  }

  public abstract read(): Bindings | null;
}

export enum Side {
  right,
  left
}
