import type { IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import { ActorQueryOperation,
  ActorQueryOperationTypedMediated,
  materializeOperation } from '@comunica/bus-query-operation';
import type { IActorTest } from '@comunica/core';
import { AsyncEvaluator, isExpressionError } from '@comunica/expression-evaluator';
import type { IActionContext, IQueryOperationResult } from '@comunica/types';
import { HashBindings } from '@incremunica/hash-bindings';
import type { Bindings } from '@incremunica/incremental-bindings-factory';
import { BindingsFactory, bindingsToString } from '@incremunica/incremental-bindings-factory';
import type { BindingsStream } from '@incremunica/incremental-types';
import { EmptyIterator, SingletonIterator, UnionIterator } from 'asynciterator';
import type { Algebra } from 'sparqlalgebrajs';

/**
 * A comunica Filter Sparqlee Query Operation Actor.
 */
export class ActorQueryOperationIncrementalFilter extends ActorQueryOperationTypedMediated<Algebra.Filter> {
  public constructor(args: IActorQueryOperationTypedMediatedArgs) {
    super(args, 'filter');
  }

  public async testOperation(operation: Algebra.Filter, context: IActionContext): Promise<IActorTest> {
    // eslint-disable-next-line no-console
    console.warn(`SPARQL Filter isn't 100% supported and doesn't have 100% test coverage`);
    if (operation.expression.expressionType === 'existence') {
      return true;
    }
    if (operation.expression.expressionType === 'operator') {
      const config = { ...ActorQueryOperation.getAsyncExpressionContext(context, this.mediatorQueryOperation) };
      const _ = new AsyncEvaluator(operation.expression, config);
      return true;
    }
    throw new Error(`Filter expression (${operation.expression.expressionType}) not yet supported!`);
  }

  public async runOperation(operation: Algebra.Filter, context: IActionContext): Promise<IQueryOperationResult> {
    let input = operation.input;
    let currentOperation = operation.expression;
    if (operation.expression.expressionType === 'operator' && operation.expression.operator === '&&') {
      currentOperation = operation.expression.args[1];
      input = {
        type: operation.type,
        input: operation.input,
        expression: <any>{
          type: operation.expression.args[0].type,
          expressionType: operation.expression.args[0].expressionType,
          operator: operation.expression.args[0].operator,
          args: operation.expression.args[0].args,
        },
      };
    }

    const outputRaw = await this.mediatorQueryOperation.mediate({ operation: input, context });
    const output = ActorQueryOperation.getSafeBindings(outputRaw);
    ActorQueryOperation.validateQueryOutput(output, 'bindings');

    const BF = new BindingsFactory();

    if (currentOperation.expressionType !== 'existence') {
      const config = { ...ActorQueryOperation.getAsyncExpressionContext(context, this.mediatorQueryOperation) };
      const evaluator = new AsyncEvaluator(currentOperation, config);

      const transform = async(item: Bindings, done: any, push: (bindings: Bindings) => void): Promise<void> => {
        try {
          const result = await evaluator.evaluateAsEBV(item);
          if (result) {
            push(item);
          }
        } catch (error: unknown) {
          // We ignore all Expression errors.
          // Other errors (likely programming mistakes) are still propagated.
          //
          // > Specifically, FILTERs eliminate any solutions that,
          // > when substituted into the expression, either result in
          // > an effective boolean value of false or produce an error.
          // > ...
          // > These errors have no effect outside of FILTER evaluation.
          // https://www.w3.org/TR/sparql11-query/#expressions
          if (isExpressionError(<Error> error)) {
            // In many cases, this is a user error, where the user should manually cast the variable to a string.
            // In order to help users debug this, we should report these errors via the logger as warnings.
            this.logWarn(context, 'Error occurred while filtering.', () => ({
              error,
              bindings: bindingsToString(item),
            }));
          } else {
            bindingsStream.emit('error', error);
          }
        }
        done();
      };

      const bindingsStream = output.bindingsStream.transform<Bindings>({ transform, autoStart: false });
      return { type: 'bindings', bindingsStream, metadata: output.metadata };
    }
    const transformMap = new Map<string, {
      count: number;
      iterator: BindingsStream;
      currentState: boolean;
    }>();

    const hashBindings = new HashBindings();

    const binder = async(bindings: Bindings, done: () => void, push: (i: BindingsStream) => void): Promise<void> => {
      const hash = hashBindings.hash(bindings);
      let hashData = transformMap.get(hash);
      if (bindings.diff) {
        if (hashData === undefined) {
          hashData = {
            count: 1,
            iterator: new EmptyIterator(),
            currentState: false,
          };
          transformMap.set(hash, hashData);

          const materializedOperation = materializeOperation(currentOperation.input, bindings);
          const intermediateOutputRaw = await this.mediatorQueryOperation.mediate({
            operation: materializedOperation,
            context,
          });
          const intermediateOutput = ActorQueryOperation.getSafeBindings(intermediateOutputRaw);

          // A `destroy` could be called on the EmptyIterator before QueryOperation mediator has finished
          if (hashData.count === 0) {
            intermediateOutput.bindingsStream.destroy();
            done();
            return;
          }

          let negBindings: Bindings;
          let posBindings: Bindings;

          if (currentOperation.not) {
            negBindings = BF.fromBindings(bindings);
            hashData.currentState = true;
            posBindings = BF.fromBindings(bindings);
            posBindings.diff = false;
          } else {
            negBindings = BF.fromBindings(bindings);
            negBindings.diff = false;
            posBindings = BF.fromBindings(bindings);
          }
          let count = 0;

          const transform = (
            item: Bindings,
            doneTransform: () => void,
            pushTransform: (val: Bindings) => void,
          ): void => {
            if (item.diff) {
              if (count === 0) {
                if (hashData === undefined) {
                  throw new Error('hashData undefined, should not happen');
                }
                hashData.currentState = !currentOperation.not;
                for (let i = 0; i < hashData.count; i++) {
                  pushTransform(posBindings);
                }
              }
              count++;
            } else if (count > 1) {
              count--;
            } else {
              count = 0;
              if (hashData === undefined) {
                throw new Error('hashData undefined, should not happen');
              }
              hashData.currentState = currentOperation.not;
              for (let i = 0; i < hashData.count; i++) {
                pushTransform(negBindings);
              }
            }
            doneTransform();
          };

          const it = intermediateOutput.bindingsStream.transform({
            transform,
            prepend: currentOperation.not ? [ bindings ] : undefined,
          });

          hashData.iterator = it;
          push(it);
        } else {
          hashData.count++;
          if (hashData.currentState) {
            push(new SingletonIterator(bindings));
          }
        }
      } else {
        if (hashData === undefined) {
          done();
          return;
        }
        if (hashData.currentState) {
          push(new SingletonIterator(bindings));
        }
        if (hashData.count === 1) {
          hashData.iterator.close();
          transformMap.delete(hash);
        }
        hashData.count--;
      }
      done();
    };

    const bindingsStream = new UnionIterator(output.bindingsStream.transform({
      transform: binder,
    }), { autoStart: false });
    return { type: 'bindings', bindingsStream, metadata: output.metadata };
  }
}
