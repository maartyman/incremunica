import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type { MediatorHashQuads } from '@comunica/bus-hash-quads';
import type { IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import {
  ActorQueryOperationTypedMediated,
} from '@comunica/bus-query-operation';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type {
  IActionContext,
  IQueryOperationResult,
  IQueryOperationResultBindings,
  BindingsStream,
  IQueryOperationResultQuads,
} from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import { getSafeBindings, getSafeQuads } from '@comunica/utils-query-operation';
import { KeysBindings } from '@incremunica/context-entries';
import type { Quad } from '@incremunica/types';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import type { Algebra } from 'sparqlalgebrajs';

/**
 * An Incremunica Distinct Hash Query Operation Actor.
 */
export class ActorQueryOperationDistinctHash extends ActorQueryOperationTypedMediated<Algebra.Distinct> {
  public readonly mediatorHashBindings: MediatorHashBindings;
  public readonly mediatorHashQuads: MediatorHashQuads;

  public constructor(args: IActorQueryOperationDistinctHashArgs) {
    super(args, 'distinct');
  }

  public async testOperation(_operation: Algebra.Distinct, _context: IActionContext): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async runOperation(operation: Algebra.Distinct, context: IActionContext): Promise<IQueryOperationResult> {
    const output = await this.mediatorQueryOperation.mediate({ operation: operation.input, context });

    if (output.type === 'quads') {
      const outputQuads: IQueryOperationResultQuads = getSafeQuads(
        output,
      );

      const quadStream: AsyncIterator<RDF.Quad> = outputQuads.quadStream.filter(await this.newHashFilterQuads(context));
      return {
        type: 'quads',
        quadStream,
        metadata: outputQuads.metadata,
      };
    }

    const outputBindings: IQueryOperationResultBindings = getSafeBindings(
      output,
    );

    const variables = (await outputBindings.metadata()).variables.map(v => v.variable);
    const bindingsStream: BindingsStream =
      <BindingsStream><any>(<AsyncIterator<Bindings>><any>outputBindings.bindingsStream)
        .filter(await this.newHashFilterBindings(context, variables));
    return {
      type: 'bindings',
      bindingsStream,
      metadata: outputBindings.metadata,
    };
  }

  /**
   * Create a new distinct filter function.
   * This will maintain an internal hash datastructure so that every bindings object only returns true once.
   * @param context The action context.
   * @param variables The variables to take into account while hashing.
   * @return {(bindings: Bindings) => boolean} A distinct filter for bindings.
   */
  public async newHashFilterBindings(
    context: IActionContext,
    variables: RDF.Variable[],
  ): Promise<(bindings: Bindings) => boolean> {
    const { hashFunction } = await this.mediatorHashBindings.mediate({ context });
    // Base comunica uses an object here but as we hash deletions incremunica uses a Map
    const hashes: Map<number, number> = new Map<number, number>();
    return (bindings: Bindings) => {
      const hash = hashFunction(bindings, variables);
      const hasMapValue = hashes.get(hash);
      const isAddition = bindings.getContextEntry(KeysBindings.isAddition) ?? true;
      if (isAddition) {
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

  /**
   * Create a new distinct filter function to hash quads.
   * This will maintain an internal hash datastructure so that every quad object only returns true once.
   * @param context The action context.
   * @return {(quad: RDF.Quad) => boolean} A distinct filter for quads.
   */
  public async newHashFilterQuads(context: IActionContext): Promise<(quad: RDF.Quad) => boolean> {
    const { hashFunction } = await this.mediatorHashQuads.mediate({ context });
    // Base comunica uses an object here but as we hash deletions incremunica uses a Map
    const hashes: Map<number, number> = new Map<number, number>();
    return (rdfQuad: RDF.Quad) => {
      const quad = <Quad>rdfQuad;
      const hash = hashFunction(quad);
      const hasMapValue = hashes.get(hash);
      quad.isAddition = quad.isAddition ?? true;
      if (quad.isAddition) {
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

export interface IActorQueryOperationDistinctHashArgs extends IActorQueryOperationTypedMediatedArgs {
  mediatorHashBindings: MediatorHashBindings;
  mediatorHashQuads: MediatorHashQuads;
}
