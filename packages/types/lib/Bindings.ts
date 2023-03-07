import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';

/**
 * An immutable solution mapping object.
 * This maps variables to a terms.
 */
export type Bindings = RDF.Bindings & {
  /**
  * An extra attribute that defines if a certain Quad is an addition or deletion.
  * diff = true => The quad is an addition.
  * diff = false => The quad is a deletion.
  */
  diff?: boolean;
};

/**
 * A stream of bindings.
 * @see Bindings
 */
export type BindingsStream = AsyncIterator<Bindings> & RDF.ResultStream<Bindings>;
