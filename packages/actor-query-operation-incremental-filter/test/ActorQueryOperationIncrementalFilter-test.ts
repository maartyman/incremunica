import type { BindingsFactory } from '@comunica/bindings-factory';
import { ActorQueryOperation } from '@comunica/bus-query-operation';
import { KeysInitQuery } from '@comunica/context-entries';
import { ActionContext, Bus } from '@comunica/core';
import * as sparqlee from '@comunica/expression-evaluator';
import { isExpressionError } from '@comunica/expression-evaluator';
import type { IQueryOperationResultBindings, Bindings } from '@comunica/types';
import {
  ActionContextKeyIsAddition,
  ActorMergeBindingsContextIsAddition,
} from '@incremunica/actor-merge-bindings-context-is-addition';
import {DevTools} from "@incremunica/dev-tools";
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import type { Algebra } from 'sparqlalgebrajs';
import { Factory, translate } from 'sparqlalgebrajs';
import { ActorQueryOperationIncrementalFilter } from '../lib';
import '@comunica/jest';
import '@incremunica/incremental-jest';
import {EventEmitter} from "events";
import type { MediatorMergeBindingsContext } from '@comunica/bus-merge-bindings-context';

const DF = new DataFactory();

function template(expr: string) {
  return `
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX fn: <https://www.w3.org/TR/xpath-functions#>
PREFIX err: <http://www.w3.org/2005/xqt-errors#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT * WHERE { ?s ?p ?o FILTER (${expr})}
`;
}

function parse(query: string): Algebra.Expression {
  const sparqlQuery = translate(template(query));
  // Extract filter expression from complete query
  return sparqlQuery.input.expression;
}

async function partialArrayifyStream<V>(stream: EventEmitter, num: number): Promise<V[]> {
  const array: V[] = [];
  for (let i = 0; i < num; i++) {
    await new Promise<void>(resolve => stream.once('data', (bindings: V) => {
      array.push(bindings);
      resolve();
    }));
  }
  return array;
}

describe('ActorQueryOperationFilterSparqlee', () => {
  let bus: any;
  let mediatorQueryOperation: any;
  const simpleSPOInput = new Factory().createBgp([ new Factory().createPattern(
    DF.variable('s'),
    DF.variable('p'),
    DF.variable('o'),
  ) ]);
  const truthyExpression = parse('"nonemptystring"');
  const operationExpression = parse('?a > 0');
  const falsyExpression = parse('""');
  const erroringExpression = parse('?a + ?a');
  const unknownExpression = {
    args: [],
    expressionType: 'operator',
    operator: 'DUMMY',
  };
  let BF: BindingsFactory;

  beforeEach(async() => {
    BF = await DevTools.createBindingsFactory(DF);
    bus = new Bus({ name: 'bus' });
    mediatorQueryOperation = {
      mediate: (arg: any) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ], { autoStart: false }),
        metadata: () => Promise.resolve({ cardinality: 3, canContainUndefs: false, variables: [ DF.variable('a') ]}),
        operated: arg,
        type: 'bindings',
      }),
    };
  });

  describe('The ActorQueryOperationFilterSparqlee module', () => {
    it('should be a function', () => {
      expect(ActorQueryOperationIncrementalFilter).toBeInstanceOf(Function);
    });

    it('should be a ActorQueryOperationFilterSparqlee constructor', () => {
      expect(new (<any> ActorQueryOperationIncrementalFilter)({ name: 'actor', bus, mediatorQueryOperation }))
        .toBeInstanceOf(ActorQueryOperationIncrementalFilter);
      expect(new (<any> ActorQueryOperationIncrementalFilter)({ name: 'actor', bus, mediatorQueryOperation }))
        .toBeInstanceOf(ActorQueryOperation);
    });

    it('should not be able to create new ActorQueryOperationFilterSparqlee objects without \'new\'', () => {
      expect(() => {
        (<any> ActorQueryOperationIncrementalFilter)();
      }).toThrow();
    });
  });

  describe('An ActorQueryOperationFilterSparqlee instance', () => {
    let actor: ActorQueryOperationIncrementalFilter;
    let factory: Factory;

    beforeEach(() => {
      const mediatorMergeBindingsContext: MediatorMergeBindingsContext = <any> {
        mediate: async() => (await new ActorMergeBindingsContextIsAddition({
          bus: new Bus({name: 'bus'}),
          name: 'actor'
        }).run(<any>{})).mergeHandlers,
      };
      actor = new ActorQueryOperationIncrementalFilter({ name: 'actor', bus, mediatorQueryOperation, mediatorMergeBindingsContext });
      factory = new Factory();
    });

    it('should test on filter operator', () => {
      const op: any = { operation: { type: 'filter', expression: operationExpression }, context: new ActionContext() };
      return expect(actor.test(op)).resolves.toBeTruthy();
    });

    it('should test on filter existence', () => {
      const op: any = { operation: { type: 'filter', expression: { expressionType: 'existence' }}, context: new ActionContext() };
      return expect(actor.test(op)).resolves.toBeTruthy();
    });

    it('should fail on unsupported operators 1', () => {
      const op: any = { operation: { type: 'filter', expression: truthyExpression }, context: new ActionContext() };
      return expect(actor.test(op)).rejects.toBeTruthy();
    });

    it('should fail on unsupported operators 2', () => {
      const op: any = { operation: { type: 'filter', expression: unknownExpression }, context: new ActionContext() };
      return expect(actor.test(op)).rejects.toBeTruthy();
    });

    it('should not test on non-filter', () => {
      const op: any = { operation: { type: 'some-other-type' }};
      return expect(actor.test(op)).rejects.toBeTruthy();
    });

    it('should return the full stream for a truthy filter', async() => {
      const op: any = { operation: { type: 'filter', input: {}, expression: truthyExpression }, context: new ActionContext() };
      const output: IQueryOperationResultBindings = <any> await actor.run(op);
      await expect(partialArrayifyStream(output.bindingsStream, 3)).resolves.toEqualBindingsArray([
        BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      expect(output.type).toBe('bindings');
      await expect(output.metadata()).resolves
        .toMatchObject({ cardinality: 3, canContainUndefs: false, variables: [ DF.variable('a') ]});
    });

    it('should return an empty stream for a falsy filter', async() => {
      const op: any = { operation: { type: 'filter', input: {}, expression: falsyExpression }, context: new ActionContext() };
      const output: IQueryOperationResultBindings = <any> await actor.run(op);
      await expect(output.bindingsStream).toEqualBindingsStream([]);
      await expect(output.metadata()).resolves
        .toMatchObject({ cardinality: 3, canContainUndefs: false, variables: [ DF.variable('a') ]});
      expect(output.type).toBe('bindings');
    });

    it('should return an empty stream when the expressions error', async() => {
      const op: any = { operation: { type: 'filter', input: {}, expression: erroringExpression }, context: new ActionContext() };
      const output: IQueryOperationResultBindings = <any> await actor.run(op);
      await expect(output.bindingsStream).toEqualBindingsStream([]);
      await expect(output.metadata()).resolves
        .toMatchObject({ cardinality: 3, canContainUndefs: false, variables: [ DF.variable('a') ]});
      expect(output.type).toBe('bindings');
    });

    it('Should log warning for an expressionError', async() => {
      // The order is very important. This item requires isExpressionError to still have it's right definition.
      const logWarnSpy = jest.spyOn(<any> actor, 'logWarn');
      const op: any = { operation: { type: 'filter', input: {}, expression: erroringExpression }, context: new ActionContext() };
      const output: IQueryOperationResultBindings = <any> await actor.run(op);
      output.bindingsStream.on('data', () => {
        // This is here to force the stream to start.
      });
      await new Promise<void>(resolve => output.bindingsStream.on('end', resolve));
      expect(logWarnSpy).toHaveBeenCalledTimes(3);
      for (const [ index, call ] of logWarnSpy.mock.calls) {
        if (index === 0) {
          const dataCB = <() => { error: any; bindings: Bindings }>call[2];
          const { error, bindings } = dataCB();
          expect(isExpressionError(error)).toBeTruthy();
          expect(bindings).toBe(`{
  "a": "\\"1\\""
}`);
        }
      }
    });

    it('should emit an error for a hard erroring filter', async() => {
      Object.defineProperty(sparqlee, 'isExpressionError', { writable: true });
      jest.spyOn((<any> sparqlee, 'isExpressionError').mockImplementation(() => false);
      const op: any = { operation: { type: 'filter', input: {}, expression: erroringExpression },        context: new ActionContext() };
      const output: IQueryOperationResultBindings = <any> await actor.run(op);
      output.bindingsStream.on('data', () => {
        // This is here to force the stream to start.
      });
      await new Promise<void>(resolve => output.bindingsStream.on('error', () => resolve()));
    });

    it('should use and respect the baseIRI from the expression context', async() => {
      const expression = parse('str(IRI(?a)) = concat("http://example.com/", ?a)');
      const context = new ActionContext({
        [KeysInitQuery.baseIRI.name]: 'http://example.com',
      });
      const op: any = { operation: { type: 'filter', input: {}, expression }, context };
      const output: IQueryOperationResultBindings = <any> await actor.run(op);
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
      ]);
      expect(output.type).toBe('bindings');
      await expect(output.metadata()).resolves
        .toMatchObject({ cardinality: 3, canContainUndefs: false, variables: [ DF.variable('a') ]});
    });

    describe('should be able to handle EXIST filters', () => {
      it('like a simple EXIST that is true', async() => {
        // The actual bgp isn't used
        const op: any = { operation: { type: 'filter', input: {}, expression: parse('EXISTS {?a a ?a}') }, context: new ActionContext() };
        const output: IQueryOperationResultBindings = <any> await actor.run(op);
        await expect(partialArrayifyStream(output.bindingsStream, 3)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
        ]);
        await expect(output.metadata()).resolves
          .toMatchObject({ cardinality: 3, canContainUndefs: false, variables: [ DF.variable('a') ]});
        expect(output.type).toBe('bindings');
      });

      it('like a simple NOT EXIST that is true', async() => {
        // The actual bgp isn't used
        const op: any = { operation: { type: 'filter', input: {}, expression: parse('NOT EXISTS {?a a ?a}') }, context: new ActionContext() };
        const output: IQueryOperationResultBindings = <any> await actor.run(op);
        await expect(partialArrayifyStream(output.bindingsStream, 6)).resolves.toBeIsomorphicBindingsArray([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(new ActionContextKeyIsAddition(), true),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(new ActionContextKeyIsAddition(), false),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(new ActionContextKeyIsAddition(), false),
        ]);
        await expect(output.metadata()).resolves
          .toMatchObject({ cardinality: 3, canContainUndefs: false, variables: [ DF.variable('a') ]});
        expect(output.type).toBe('bindings');
      });
    });
  });
});
