import type { AsyncIterator, MapFunction, SourcedIteratorOptions } from 'asynciterator';
import { identity, MappingIterator } from 'asynciterator';

/**
 * A TransformIterator with a callback for when this iterator is started and closed in any way.
 */
export class StartClosableMappingIterator<S, D = S> extends MappingIterator<S, D> {
  private readonly onStart: () => void;
  private readonly onClose: () => void;

  public constructor(
    source: AsyncIterator<S>,
    map: MapFunction<S, D> = <MapFunction<S, D>>identity,
    options?: SourcedIteratorOptions & {
      onStart: () => void;
      onClose: () => void;
    },
  ) {
    super(source, map, options);
    this.onStart = options?.onStart ?? (() => {});
    this.onClose = options?.onClose ?? (() => {});
  }

  public override read(): D | null {
    this.onStart();
    this.read = super.read;
    return super.read();
  }

  protected override _end(destroy: boolean): void {
    this.onClose();
    super._end(destroy);
  }
}
