import type * as RDF from '@rdfjs/types';
import { Map } from 'immutable';
import { DataFactory } from 'rdf-data-factory';
import { Bindings } from './Bindings';

/**
 * A Bindings factory that provides Bindings backed by immutable.js.
 */
export class BindingsFactory {
  private readonly dataFactory: RDF.DataFactory;

  public constructor(dataFactory: RDF.DataFactory = new DataFactory()) {
    this.dataFactory = dataFactory;
  }

  public bindings(entries: [RDF.Variable, RDF.Term][] = [], diff = true): Bindings {
    return new Bindings(this.dataFactory, Map(entries.map(([ key, value ]) => [ key.value, value ])), diff);
  }

  public fromBindings(bindings: Bindings): Bindings {
    return this.bindings([ ...bindings ], bindings.diff);
  }

  public fromRecord(record: Record<string, RDF.Term>, diff: boolean = true): Bindings {
    return this.bindings(Object.entries(record).map(([ key, value ]) => [ this.dataFactory.variable!(key), value ]), diff);
  }
}
