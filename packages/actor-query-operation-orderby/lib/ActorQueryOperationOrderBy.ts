import type { MediatorExpressionEvaluatorFactory } from '@comunica/bus-expression-evaluator-factory';
import type { IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import { ActorQueryOperationTypedMediated } from '@comunica/bus-query-operation';
import type { MediatorTermComparatorFactory } from '@comunica/bus-term-comparator-factory';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type { BindingsStream, IActionContext, IQueryOperationResult } from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import { bindingsToCompactString } from '@comunica/utils-bindings-factory';
import { isExpressionError } from '@comunica/utils-expression-evaluator';
import { getSafeBindings } from '@comunica/utils-query-operation';
import { KeysBindings } from '@incremunica/context-entries';
import type * as RDF from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import { Algebra } from 'sparqlalgebrajs';
import { IndexedSortTree } from './IndexedSortTree';

export interface IAnnotatedBinding {
  bindings: Bindings;
  result: (RDF.Term | undefined)[];
  hash: string;
}

/**
 * An incremunica OrderBy Query Operation Actor.
 */
export class ActorQueryOperationOrderBy extends ActorQueryOperationTypedMediated<Algebra.OrderBy> {
  private readonly mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
  private readonly mediatorTermComparatorFactory: MediatorTermComparatorFactory;

  public constructor(args: IActorQueryOperationOrderBySparqleeArgs) {
    super(args, 'orderby');
    this.mediatorExpressionEvaluatorFactory = args.mediatorExpressionEvaluatorFactory;
    this.mediatorTermComparatorFactory = args.mediatorTermComparatorFactory;
  }

  public async testOperation(): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async runOperation(operation: Algebra.OrderBy, context: IActionContext):
  Promise<IQueryOperationResult> {
    const outputRaw = await this.mediatorQueryOperation.mediate({ operation: operation.input, context });
    const output = getSafeBindings(outputRaw);
    const variables = (await output.metadata()).variables.map(v => v.variable);

    let bindingsStream = <AsyncIterator<Bindings>><any>output.bindingsStream;

    // Sorting backwards since the first one is the most important therefore should be ordered last.
    const orderByEvaluator = await this.mediatorTermComparatorFactory.mediate({ context });

    let annotatedBindingsStream = bindingsStream.map<IAnnotatedBinding>((bindings: Bindings) =>
      ({ bindings, result: [], hash: bindingsToCompactString(bindings, variables) }));
    const isAscending = [];
    for (let expr of operation.expressions) {
      isAscending.push(this.isAscending(expr));
      expr = this.extractSortExpression(expr);
      // Transform the stream by annotating it with the expr result
      const evaluator = await this.mediatorExpressionEvaluatorFactory
        .mediate({ algExpr: expr, context });

      const transform = async(
        annotatedBinding: IAnnotatedBinding,
        next: () => void,
        push: (result: IAnnotatedBinding) => void,
      ): Promise<void> => {
        try {
          const result = await evaluator.evaluate(annotatedBinding.bindings);
          annotatedBinding.result.push(result);
          push(annotatedBinding);
        } catch (error: unknown) {
          // We ignore all Expression errors.
          // Other errors (likely programming mistakes) are still propagated.
          if (!isExpressionError(<Error> error)) {
            bindingsStream.destroy(<Error> error);
            next();
            return;
          }
          annotatedBinding.result.push(undefined);
          push(annotatedBinding);
        }
        next();
      };
      // eslint-disable-next-line ts/no-misused-promises
      annotatedBindingsStream = annotatedBindingsStream.transform<IAnnotatedBinding>({ transform, autoStart: false });
    }

    const index = new IndexedSortTree(orderByEvaluator, isAscending);
    bindingsStream = annotatedBindingsStream.map((annotatedBindings) => {
      try {
        const bindingsOrderData =
          (annotatedBindings.bindings.getContextEntry(KeysBindings.isAddition) ?? true) ?
            index.insert(annotatedBindings).data :
            index.remove(annotatedBindings).data;
        return annotatedBindings.bindings.setContextEntry(KeysBindings.order, bindingsOrderData);
      } catch (error) {
        bindingsStream.destroy(<any>error);
        return null;
      }
    });

    return {
      type: 'bindings',
      bindingsStream: <BindingsStream><any>bindingsStream,
      metadata: output.metadata,
    };
  }

  // Remove descending operator if necessary
  private extractSortExpression(expr: Algebra.Expression): Algebra.Expression {
    const { expressionType, operator } = expr;
    if (expressionType !== Algebra.expressionTypes.OPERATOR) {
      return expr;
    }
    return operator === 'desc' ?
      expr.args[0] :
      expr;
  }

  private isAscending(expr: Algebra.Expression): boolean {
    const { expressionType, operator } = expr;
    if (expressionType !== Algebra.expressionTypes.OPERATOR) {
      return true;
    }
    return operator !== 'desc';
  }
}

export interface IActorQueryOperationOrderBySparqleeArgs extends IActorQueryOperationTypedMediatedArgs {
  mediatorExpressionEvaluatorFactory: MediatorExpressionEvaluatorFactory;
  mediatorTermComparatorFactory: MediatorTermComparatorFactory;
}
