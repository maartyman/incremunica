import type { IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import {
  ActorQueryOperation,
  ActorQueryOperationTypedMediated,
} from '@comunica/bus-query-operation';
import type { IActorTest } from '@comunica/core';
import type { IActionContext, IQueryOperationResult, IQueryOperationResultBindings } from '@comunica/types';
import { HashBindings } from '@incremunica/hash-bindings';
import type { Bindings } from '@comunica/bindings-factory';
import type { BindingsStream } from '@comunica/types';
import type { Algebra } from 'sparqlalgebrajs';
import {ActionContextKeyIsAddition} from "@incremunica/actor-merge-bindings-context-is-addition";

/**
 * An Incremunica Distinct Hash Query Operation Actor.
 */
export class ActorQueryOperationIncrementalDistinctHash extends ActorQueryOperationTypedMediated<Algebra.Distinct> {
  public constructor(args: IActorQueryOperationDistinctHashArgs) {
    super(args, 'distinct');
  }

  public async testOperation(operation: Algebra.Distinct, context: IActionContext): Promise<IActorTest> {
    return true;
  }

  public async runOperation(operation: Algebra.Distinct, context: IActionContext): Promise<IQueryOperationResult> {
    const output: IQueryOperationResultBindings = ActorQueryOperation.getSafeBindings(
      await this.mediatorQueryOperation.mediate({ operation: operation.input, context }),
    );
    const bindingsStream: BindingsStream = <BindingsStream>output.bindingsStream.filter(
      this.newHashFilter(),
    );
    return {
      type: 'bindings',
      bindingsStream,
      metadata: output.metadata,
    };
  }

  /**
   * Create a new distinct filter function.
   * This will maintain an internal hash datastructure so that every bindings object only returns true once.
   * @return {(bindings: Bindings) => boolean} A distinct filter for bindings.
   */
  public newHashFilter(): (bindings: Bindings) => boolean {
    const hashBindings = new HashBindings();
    // Base comunica uses an object here but as we hash deletions incremunica uses a Map
    const hashes: Map<string, number> = new Map<string, number>();
    return (bindings: Bindings) => {
      const hash: string = hashBindings.hash(bindings);
      const hasMapValue = hashes.get(hash);
      if (bindings.getContextEntry(new ActionContextKeyIsAddition())) {
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

export interface IActorQueryOperationDistinctHashArgs extends IActorQueryOperationTypedMediatedArgs {}
