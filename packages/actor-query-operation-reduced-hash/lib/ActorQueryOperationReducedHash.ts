import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type { IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import { ActorQueryOperationTypedMediated } from '@comunica/bus-query-operation';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type {
  BindingsStream,
  IActionContext,
  IQueryOperationResult,
  IQueryOperationResultBindings,
} from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import { getSafeBindings } from '@comunica/utils-query-operation';
import { KeysBindings } from '@incremunica/context-entries';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import type { Algebra } from 'sparqlalgebrajs';

/**
 * An Incremunica Reduced Hash Query Operation Actor.
 */
export class ActorQueryOperationReducedHash extends ActorQueryOperationTypedMediated<Algebra.Reduced> {
  public readonly mediatorHashBindings: MediatorHashBindings;

  public constructor(args: IActorQueryOperationReducedHashArgs) {
    super(args, 'reduced');
  }

  public async testOperation(_operation: Algebra.Reduced, _context: IActionContext): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async runOperation(operation: Algebra.Reduced, context: IActionContext): Promise<IQueryOperationResult> {
    const output: IQueryOperationResultBindings = getSafeBindings(
      await this.mediatorQueryOperation.mediate({ operation: operation.input, context }),
    );
    const variables = (await output.metadata()).variables.map(v => v.variable);
    const bindingsStream: BindingsStream =
      <BindingsStream><any>(<AsyncIterator<Bindings>><any>output.bindingsStream)
        .filter(await this.newHashFilter(context, variables));
    return {
      type: 'bindings',
      bindingsStream,
      metadata: output.metadata,
    };
  }

  /**
   * Create a new distinct filter function.
   * This will maintain an internal hash datastructure so that every bindings object only returns true once.
   * @param context The action context.
   * @param variables The variables to take into account while hashing.
   * @return {(bindings: Bindings) => boolean} A distinct filter for bindings.
   */
  public async newHashFilter(
    context: IActionContext,
    variables: RDF.Variable[],
  ): Promise<(bindings: Bindings) => boolean> {
    const { hashFunction } = await this.mediatorHashBindings.mediate({ context });
    const hashes: Map<number, number> = new Map<number, number>();
    return (bindings: Bindings) => {
      const hash = hashFunction(bindings, variables);
      const hasMapValue = hashes.get(hash);
      if (bindings.getContextEntry(KeysBindings.isAddition) ?? true) {
        if (hasMapValue) {
          hashes.set(hash, hasMapValue + 1);
          return false;
        }
        hashes.set(hash, 1);
        return true;
      }
      if (!hasMapValue) {
        return false;
      }
      if (hasMapValue === 1) {
        hashes.delete(hash);
        return true;
      }
      hashes.set(hash, hasMapValue - 1);
      return false;
    };
  }
}

export interface IActorQueryOperationReducedHashArgs extends IActorQueryOperationTypedMediatedArgs {
  mediatorHashBindings: MediatorHashBindings;
}
