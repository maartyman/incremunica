import type { Bindings } from '@incremunica/incremental-types';
import { termToString } from 'rdf-string';

/**
 * Stringify a bindings object.
 * @param bindings A bindings object.
 * @param includeDiff Include the diff of the binding in the string.
 */
export function bindingsToString(bindings: Bindings, includeDiff = false): string {
  const raw: Record<string, string> = {};
  for (const key of bindings.keys()) {
    raw[key.value] = termToString(bindings.get(key))!;
  }
  if (includeDiff) {
    const total: Record<string, any> = {};
    total.bindings = raw;
    total.diff = bindings.diff;
    return JSON.stringify(total, null, '  ');
  }
  return JSON.stringify(raw, null, '  ');
}
