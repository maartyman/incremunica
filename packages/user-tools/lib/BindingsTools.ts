import type { Bindings } from '@comunica/types';
import type { Bindings as BindingsFromBindingsFactory } from '@comunica/utils-bindings-factory';
import { KeysBindings } from '@incremunica/context-entries';

/**
 * Check if the given bindings are an addition.
 * @param bindings
 */
export function isAddition(bindings: Bindings): boolean {
  return (<BindingsFromBindingsFactory>bindings).getContextEntry(KeysBindings.isAddition) ?? true;
}

/**
 * Get the index of the given bindings.
 * @param bindings
 */
export function getBindingsIndex(bindings: Bindings): number {
  return (<BindingsFromBindingsFactory>bindings).getContextEntry(KeysBindings.order)?.index ?? -1;
}
