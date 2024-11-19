import type { MediatorHashBindings } from '@comunica/bus-hash-bindings';
import type { MediatorMergeBindingsContext } from '@comunica/bus-merge-bindings-context';
import type { IActorQueryOperationTypedMediatedArgs } from '@comunica/bus-query-operation';
import { ActorQueryOperationTypedMediated } from '@comunica/bus-query-operation';
import { KeysInitQuery } from '@comunica/context-entries';
import type { IActorTest, TestResult } from '@comunica/core';
import { passTestVoid } from '@comunica/core';
import type {
  BindingsStream,
  ComunicaDataFactory,
  IActionContext,
  IQueryOperationResult,
  MetadataVariable,
} from '@comunica/types';
import { getSafeBindings } from '@comunica/utils-query-operation';
import type { MediatorBindingsAggregatorFactory } from '@incremunica/bus-bindings-aggregator-factory';
import type { Algebra } from 'sparqlalgebrajs';
import { GroupIterator } from './GroupsIterator';

/**
 * An incremunica Group Query Operation Actor.
 */
export class ActorQueryOperationGroup extends ActorQueryOperationTypedMediated<Algebra.Group> {
  public readonly mediatorMergeBindingsContext: MediatorMergeBindingsContext;
  public readonly mediatorBindingsAggregatorFactory: MediatorBindingsAggregatorFactory;
  public readonly mediatorHashBindings: MediatorHashBindings;

  public constructor(args: IActorQueryOperationGroupArgs) {
    super(args, 'group');
    this.mediatorBindingsAggregatorFactory = args.mediatorBindingsAggregatorFactory;
    this.mediatorHashBindings = args.mediatorHashBindings;
    this.mediatorMergeBindingsContext = args.mediatorMergeBindingsContext;
  }

  public async testOperation(): Promise<TestResult<IActorTest>> {
    return passTestVoid();
  }

  public async runOperation(operation: Algebra.Group, context: IActionContext): Promise<IQueryOperationResult> {
    const dataFactory: ComunicaDataFactory = context.getSafe(KeysInitQuery.dataFactory);

    // Get result stream for the input query
    const { input, aggregates } = operation;
    const outputRaw = await this.mediatorQueryOperation.mediate({ operation: input, context });
    const output = getSafeBindings(outputRaw);

    const { hashFunction } = await this.mediatorHashBindings.mediate({ context });

    // The variables in scope are the variables on which we group, i.e. pattern.variables.
    // For 'GROUP BY ?x, ?z', this is [?x, ?z], for 'GROUP by expr(?x) as ?e' this is [?e].
    // But also in scope are the variables defined by the aggregations, since GROUP has to handle this.
    const variables: MetadataVariable[] = [
      ...operation.variables,
      ...aggregates.map(agg => agg.variable),
    ].map(variable => ({ variable, canBeUndef: false }));

    const groupVariables = new Set(operation.variables.map(x => x.value));
    const variablesInner = (await output.metadata()).variables.map(v => v.variable);

    const bindingsStream = <BindingsStream><any> new GroupIterator(
      output.bindingsStream,
      context,
      operation,
      dataFactory,
      this.mediatorBindingsAggregatorFactory,
      groupVariables,
      variablesInner,
      hashFunction,
    );

    return {
      type: 'bindings',
      bindingsStream,
      metadata: async() => ({ ...await output.metadata(), variables }),
    };
  }
}

export interface IActorQueryOperationGroupArgs extends IActorQueryOperationTypedMediatedArgs {
  /**
   * A mediator for creating binding context merge handlers
   */
  mediatorMergeBindingsContext: MediatorMergeBindingsContext;
  mediatorBindingsAggregatorFactory: MediatorBindingsAggregatorFactory;
  mediatorHashBindings: MediatorHashBindings;
}
