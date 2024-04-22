import type { Bindings } from '@incremunica/incremental-types';
import type * as RDF from '@rdfjs/types';
import { termToString } from 'rdf-string';

export class HashBindings {
  private readonly initializedWithVariables: boolean;
  private readonly variables = new Set<string>();

  public constructor(variables?: RDF.Variable[]) {
    this.initializedWithVariables = variables !== undefined;
    if (variables) {
      for (const variable of variables) {
        this.variables.add(variable.value);
      }
    }
  }

  public hash(bindings: Bindings): string {
    if (!this.initializedWithVariables) {
      for (const key of bindings.keys()) {
        if (!this.variables.has(key.value)) {
          this.variables.add(key.value);
        }
      }
    }

    let hash = '';
    for (const key of this.variables.keys()) {
      const binding = bindings.get(key);
      if (binding) {
        hash += `${key}:<${termToString(bindings.get(key))}>\n`;
      }
    }

    return hash;
  }
}
