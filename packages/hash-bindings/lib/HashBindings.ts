import type { Bindings } from '@incremunica/incremental-types';
import { termToString } from 'rdf-string';

export class HashBindings {
  private readonly variables = new Map<string, void>();
  public hash(bindings: Bindings): string {
    for (const key of bindings.keys()) {
      if (!this.variables.has(key.value)) {
        this.variables.set(key.value);
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
