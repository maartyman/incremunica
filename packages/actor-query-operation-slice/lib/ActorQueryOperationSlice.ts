import type { IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import {
  ActorQueryOperationTypedMediated,
} from '@comunica/bus-query-operation';
import { KeysInitQuery, KeysQueryOperation } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type {
  IQueryOperationResult,
  IQueryOperationResultBindings,
  IQueryOperationResultQuads,
  IQueryOperationResultStream,
  IMetadata,
  IActionContext,
  BindingsStream,
} from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import { bindingsToString } from '@comunica/utils-bindings-factory';
import { KeysBindings } from '@incremunica/context-entries';
import type { Quad } from '@incremunica/types';
import type { AsyncIterator } from 'asynciterator';
import { termToString } from 'rdf-string';
import type { Algebra } from 'sparqlalgebrajs';

/**
 * An Incremunica Slice Query Operation Actor.
 */
export class ActorQueryOperationSlice extends ActorQueryOperationTypedMediated<Algebra.Slice> {
  public constructor(args: IActorQueryOperationTypedMediatedArgs) {
    super(args, 'slice');
  }

  public async testOperation(_operation: Algebra.Slice, _context: IActionContext): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async runOperation(operation: Algebra.Slice, context: IActionContext): Promise<IQueryOperationResult> {
    // Add limit indicator to the context, which can be used for query planning
    // eslint-disable-next-line unicorn/explicit-length-check
    if (operation.length) {
      context = context.set(KeysQueryOperation.limitIndicator, operation.length);
    }
    const dataFactory = context.get(KeysInitQuery.dataFactory)!;

    // Resolve the input
    const output: IQueryOperationResult = await this.mediatorQueryOperation
      .mediate({ operation: operation.input, context });

    if (output.type === 'bindings') {
      const bindingsStream = <BindingsStream><any> this.sliceStream<Bindings>(
        <AsyncIterator<Bindings>><any>output.bindingsStream,
        operation,
        bindings => bindingsToString(bindings),
        bindings => bindings.getContextEntry(KeysBindings.isAddition) ?? true,
        bindings => bindings.setContextEntry(KeysBindings.isAddition, false),
      );
      return <IQueryOperationResultBindings> {
        type: 'bindings',
        bindingsStream,
        metadata: this.sliceMetadata(output, operation),
      };
    }

    if (output.type === 'quads') {
      const quadStream = <any> this.sliceStream<Quad>(
        <AsyncIterator<Quad>><any>output.quadStream,
        operation,
        quad => termToString(quad),
        quad => quad.isAddition ?? true,
        (quad) => {
          const newQuad = <Quad>dataFactory.quad(quad.subject, quad.predicate, quad.object, quad.graph);
          newQuad.isAddition = false;
          return newQuad;
        },
      );
      return <IQueryOperationResultQuads> {
        type: 'quads',
        quadStream,
        metadata: this.sliceMetadata(output, operation),
      };
    }

    // In all other cases, return the result as-is.
    return output;
  }

  // Slice the stream based on the pattern values
  private sliceStream<T extends Bindings | Quad>(
    stream: AsyncIterator<T>,
    pattern: Algebra.Slice,
    hashFunction: (item: T) => string,
    isAdditionFunction: (item: T) => boolean,
    makeDeletion: (item: T) => T,
  ): AsyncIterator<T> {
    // eslint-disable-next-line unicorn/explicit-length-check
    const hasLength: boolean = Boolean(pattern.length) || pattern.length === 0;
    const { start } = pattern;
    const length = hasLength ? pattern.length! : Number.POSITIVE_INFINITY;

    const addElement = (map: Map<string, { element: T; count: number }>, hash: string, element: T): void => {
      const mapValue = map.get(hash);
      if (mapValue === undefined) {
        map.set(hash, { element, count: 1 });
      } else {
        mapValue.count++;
      }
    };

    const deleteElement = (map: Map<string, { element: T; count: number }>, hash: string): boolean => {
      const mapValue = map.get(hash);
      if (mapValue === undefined) {
        return false;
      }
      if (mapValue.count === 1) {
        map.delete(hash);
      } else {
        mapValue.count--;
      }
      return true;
    };

    const deleteRandomElement = (map: Map<string, { element: T; count: number }>): [string, T] => {
      const [ key, value ]: [string, { element: T; count: number }] = map.entries().next().value;
      const mapValue = map.get(key)!;
      if (mapValue.count === 1) {
        map.delete(key);
      } else {
        mapValue.count--;
      }
      return [ key, value.element ];
    };

    const result = new Map<string, { element: T; count: number }>();
    const overflow = new Map<string, { element: T; count: number }>();
    let resultSize = 0;
    let overflowSize = 0;
    const transform = (incomingElement: T, done: () => void, push: (result: T) => void): void => {
      const hash = hashFunction(incomingElement);
      const isAddition = isAdditionFunction(incomingElement);
      if (isAddition) {
        if ((overflowSize < start) || (resultSize >= length)) {
          addElement(overflow, hash, incomingElement);
          overflowSize++;
          done();
          return;
        }
        addElement(result, hash, incomingElement);
        resultSize++;
        push(incomingElement);
        done();
        return;
      }
      // If a deletion first check overflow
      if (deleteElement(overflow, hash)) {
        if (overflowSize <= start && resultSize > 0) {
          const [ deletedKey, deletedElement ] = deleteRandomElement(result);
          push(makeDeletion(deletedElement));
          resultSize--;
          addElement(overflow, deletedKey, deletedElement);
          done();
          return;
        }
        overflowSize--;
        done();
        return;
      }
      // Otherwise check the result
      if (deleteElement(result, hash)) {
        push(makeDeletion(incomingElement));
        if (overflowSize > start) {
          const [ deletedKey, deletedElement ] = deleteRandomElement(overflow);
          push(deletedElement);
          overflowSize--;
          addElement(result, deletedKey, deletedElement);
          done();
          return;
        }
        resultSize--;
        done();
        return;
      }
      stream.destroy(new Error(`Deletion ${hash}, has not been added.`));
      done();
    };

    return stream.transform({
      transform,
      autoStart: false,
    });
  }

  // If we find metadata, apply slicing on the total number of items
  private sliceMetadata(
    output: IQueryOperationResultStream<any, any>,
    pattern: Algebra.Slice,
  ): () => Promise<IMetadata<any>> {
    // eslint-disable-next-line unicorn/explicit-length-check
    const hasLength: boolean = Boolean(pattern.length) || pattern.length === 0;
    return () => (<() => Promise<IMetadata<any>>>output.metadata)()
      .then((subMetadata) => {
        const cardinality = { ...subMetadata.cardinality };
        if (Number.isFinite(cardinality.value)) {
          cardinality.value = Math.max(0, cardinality.value - pattern.start);
          if (hasLength) {
            cardinality.value = Math.min(cardinality.value, pattern.length!);
          }
        }
        return { ...subMetadata, cardinality };
      });
  }
}
