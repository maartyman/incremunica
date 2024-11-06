import '@incremunica/incremental-jest';
import EventEmitter = require('events');
import type { IActionQueryOperation, MediatorQueryOperation } from '@comunica/bus-query-operation';
import { getContextSources } from '@comunica/bus-rdf-resolve-quad-pattern';
import type { BindingsStream, IJoinEntry, IQueryOperationResultBindings } from '@comunica/types';
import type { BindingsFactory } from '@comunica/utils-bindings-factory';
import { MetadataValidationState } from '@comunica/utils-metadata';
import { KeysBindings } from '@incremunica/context-entries';
import { DevTools } from '@incremunica/dev-tools';
import type * as RDF from '@rdfjs/types';
import arrayifyStream from 'arrayify-stream';
import { ArrayIterator, EmptyIterator, WrappingIterator } from 'asynciterator';
import { promisifyEventEmitter } from 'event-emitter-promisify/dist';
import type { Store } from 'n3';
import { DataFactory } from 'rdf-data-factory';
import { Transform } from 'readable-stream';
import { Factory } from 'sparqlalgebrajs';
import { DeltaQueryIterator } from '../lib/DeltaQueryIterator';

const streamifyArray = require('streamify-array');

const DF = new DataFactory();
const FACTORY = new Factory();

async function partialArrayifyStream(stream: EventEmitter, num: number): Promise<any[]> {
  const array: any[] = [];
  for (let i = 0; i < num; i++) {
    await new Promise<void>(resolve => stream.once('data', (bindings: any) => {
      array.push(bindings);
      resolve();
    }));
  }
  return array;
}

function nullifyVariables(term?: RDF.Term): RDF.Term | undefined {
  return !term || term.termType === 'Variable' ? undefined : term;
}

describe('DeltaQueryIterator', () => {
  let mediatorQueryOperation: MediatorQueryOperation;
  let mediateFunc: jest.Mock;
  let BF: BindingsFactory;

  beforeEach(async() => {
    BF = await DevTools.createTestBindingsFactory(DF);

    mediateFunc = jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
      const sources = getContextSources(arg.context);

      // TODO check if this is needed
      // expect(sources).toBeDefined();
      if (sources === undefined) {
        return {
          bindingsStream: new EmptyIterator(),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 0 },
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: false,
              },
              {
                variable: DF.variable('a'),
                canBeUndef: false,
              },
            ],
          }),
          type: 'bindings',
        };
      }

      const bindingstream: BindingsStream = new ArrayIterator((<Store>sources[0]).match(
        // @ts-expect-error
        nullifyVariables(arg.operation.subject),
        nullifyVariables(arg.operation.predicate),
        nullifyVariables(arg.operation.object),
        null,
      )).map((quad) => {
        if (quad.predicate.value === 'ex:p2') {
          return BF.bindings([]);
        }
        return BF.bindings([
          [ DF.variable('b'), quad.object ],
        ]);
      });

      return {
        bindingsStream: bindingstream,
        metadata: () => Promise.resolve({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 1 },
          variables: [{
            variable: DF.variable('bound'),
            canBeUndef: false,
          }],
        }),
        type: 'bindings',
      };
    });

    mediatorQueryOperation = <any>{
      mediate: mediateFunc,
    };
  });

  it('should join two entries', async() => {
    const action: IJoinEntry[] = [
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
              [ DF.variable('b'), DF.namedNode('ex:b2') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 3 },
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: false,
              },
              {
                variable: DF.variable('b'),
                canBeUndef: false,
              },
            ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 1 },
            variables: [{
              variable: DF.variable('a'),
              canBeUndef: false,
            }],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      mediatorQueryOperation,
      DF,
      FACTORY,
      BF,
    );

    await expect(arrayifyStream(delta)).resolves.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]);

    expect(mediateFunc).toHaveBeenCalledTimes(4);
  });

  it('should join two entries with deletions', async() => {
    const action: IJoinEntry[] = [
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
              [ DF.variable('b'), DF.namedNode('ex:b2') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]).setContextEntry(KeysBindings.isAddition, false),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 3 },
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: false,
              },
              {
                variable: DF.variable('b'),
                canBeUndef: false,
              },
            ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
            ]).setContextEntry(KeysBindings.isAddition, false),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 1 },
            variables: [{
              variable: DF.variable('a'),
              canBeUndef: false,
            }],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      mediatorQueryOperation,
      DF,
      FACTORY,
      BF,
    );

    await expect(arrayifyStream(delta)).resolves.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]).setContextEntry(KeysBindings.isAddition, false),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ]).setContextEntry(KeysBindings.isAddition, false),
    ]);

    expect(mediateFunc).toHaveBeenCalledTimes(6);
  });

  it('should join two slow entries', async() => {
    const transform = new Transform({
      transform(quad: any, _encoding: any, callback: (arg0: null, arg1: any) => any) {
        return callback(null, quad);
      },
      objectMode: true,
    });

    const stream = streamifyArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]),
    ], { autoStart: false }).pipe(transform, { end: false });

    const it1 = new WrappingIterator(stream);

    const it2 = new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ], { autoStart: false });

    const action: IJoinEntry[] = [
      {
        output: <any>{
          bindingsStream: it1,
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 3 },
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: false,
              },
              {
                variable: DF.variable('b'),
                canBeUndef: false,
              },
            ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any>{
          bindingsStream: it2,
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 1 },
            variables: [{
              variable: DF.variable('a'),
              canBeUndef: false,
            }],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      mediatorQueryOperation,
      DF,
      FACTORY,
      BF,
    );

    await expect(partialArrayifyStream(delta, 1)).resolves.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]),
    ]);

    stream.push(
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ]),
    );
    stream.end();

    await expect(arrayifyStream(delta)).resolves.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ]);

    expect(mediateFunc).toHaveBeenCalledTimes(4);
  });

  it('should keep reading the bindingsStream if the bindings can\'t be bound to the quad', async() => {
    const action: IJoinEntry[] = [
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 3 },
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: false,
              },
              {
                variable: DF.variable('b'),
                canBeUndef: false,
              },
            ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('c'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([
              [ DF.variable('d'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 1 },
            variables: [{
              variable: DF.variable('a'),
              canBeUndef: false,
            }],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      mediatorQueryOperation,
      DF,
      FACTORY,
      BF,
    );

    await expect(arrayifyStream(delta)).resolves.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]),
    ]);

    expect(mediateFunc).toHaveBeenCalledTimes(2);
  });

  it('should keep reading the bindingsSteam and break if it has a null', async() => {
    const action: IJoinEntry[] = [
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 3 },
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: false,
              },
              {
                variable: DF.variable('b'),
                canBeUndef: false,
              },
            ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('c'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
            null,
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 1 },
            variables: [{
              variable: DF.variable('a'),
              canBeUndef: false,
            }],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      mediatorQueryOperation,
      DF,
      FACTORY,
      BF,
    );

    await expect(arrayifyStream(delta)).resolves.toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]),
    ]);

    expect(mediateFunc).toHaveBeenCalledTimes(2);
  });

  it('should destroy entries on end', async() => {
    const it1 = new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ], { autoStart: false });

    const it2 = new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ], { autoStart: false });

    const action: IJoinEntry[] = [
      {
        output: <any>{
          bindingsStream: it1,
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 3 },
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: false,
              },
              {
                variable: DF.variable('b'),
                canBeUndef: false,
              },
            ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any>{
          bindingsStream: it2,
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 1 },
            variables: [{
              variable: DF.variable('a'),
              canBeUndef: false,
            }],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      mediatorQueryOperation,
      DF,
      FACTORY,
      BF,
    );

    delta.close();

    await new Promise<void>(resolve => setTimeout(() => resolve(), 100));

    expect(it1.destroyed).toBeTruthy();
    expect(it2.destroyed).toBeTruthy();
  });

  it('should handle destroyed entry', async() => {
    const it1 = new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ], { autoStart: false });

    const it2 = new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
      ]).setContextEntry(KeysBindings.isAddition, true),
    ], { autoStart: false });

    const action: IJoinEntry[] = [
      {
        output: <any>{
          bindingsStream: it1,
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 3 },
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: false,
              },
              {
                variable: DF.variable('b'),
                canBeUndef: false,
              },
            ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any>{
          bindingsStream: it2,
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 1 },
            variables: [{
              variable: DF.variable('a'),
              canBeUndef: false,
            }],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      mediatorQueryOperation,
      DF,
      FACTORY,
      BF,
    );

    expect(() => {
      it1.destroy(new Error('test'));
    }).toThrow('test');
  });

  it('should internally return null when the bindings can\'t be merged', async() => {
    const mediateFunc = jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
      return {
        bindingsStream: new ArrayIterator([
          BF.bindings([
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]).setContextEntry(KeysBindings.isAddition, true),
        ]),
        metadata: () => Promise.resolve({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 1 },
          variables: [{
            variable: DF.variable('bound'),
            canBeUndef: false,
          }],
        }),
        type: 'bindings',
      };
    });

    mediatorQueryOperation = <any>{
      mediate: mediateFunc,
    };

    const action: IJoinEntry[] = [
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 3 },
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: false,
              },
              {
                variable: DF.variable('b'),
                canBeUndef: false,
              },
            ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 1 },
            variables: [{
              variable: DF.variable('a'),
              canBeUndef: false,
            }],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      mediatorQueryOperation,
      DF,
      FACTORY,
      BF,
    );

    await expect(arrayifyStream(delta)).resolves.toBeIsomorphicBindingsArray([]);

    expect(mediateFunc).toHaveBeenCalledTimes(2);
  });

  it('should join three entries', async() => {
    mediateFunc = jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
      return {
        bindingsStream: new EmptyIterator(),
        metadata: () => Promise.resolve({
          state: new MetadataValidationState(),
          cardinality: { type: 'estimate', value: 1 },
          variables: [{
            variable: DF.variable('bound'),
            canBeUndef: false,
          }],
        }),
        type: 'bindings',
      };
    });

    mediatorQueryOperation = <any>{
      mediate: mediateFunc,
    };

    const action: IJoinEntry[] = [
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 3 },
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: false,
              },
              {
                variable: DF.variable('b'),
                canBeUndef: false,
              },
            ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 1 },
            variables: [{
              variable: DF.variable('a'),
              canBeUndef: false,
            }],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 1 },
            variables: [{
              variable: DF.variable('a'),
              canBeUndef: false,
            }],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('b'), DF.namedNode('ex:p3'), DF.namedNode('ex:o2')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      mediatorQueryOperation,
      DF,
      FACTORY,
      BF,
    );

    delta.on('data', () => {});

    await promisifyEventEmitter(delta);

    expect(mediateFunc).toHaveBeenCalledTimes(3);
  });

  it('should handle a failed sub query', async() => {
    mediateFunc = jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
      throw new Error('test');
    });

    mediatorQueryOperation = <any>{
      mediate: mediateFunc,
    };

    const action: IJoinEntry[] = [
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 3 },
            variables: [
              {
                variable: DF.variable('a'),
                canBeUndef: false,
              },
              {
                variable: DF.variable('b'),
                canBeUndef: false,
              },
            ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any>{
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]).setContextEntry(KeysBindings.isAddition, true),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            state: new MetadataValidationState(),
            cardinality: { type: 'estimate', value: 1 },
            variables: [{
              variable: DF.variable('a'),
              canBeUndef: false,
            }],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      mediatorQueryOperation,
      DF,
      FACTORY,
      BF,
    );

    delta.on('data', () => {});

    await promisifyEventEmitter(delta);

    expect(mediateFunc).toHaveBeenCalledTimes(2);
  });
});
