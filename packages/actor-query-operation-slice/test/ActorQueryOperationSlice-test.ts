import { ActorQueryOperation } from '@comunica/bus-query-operation';
import { KeysQueryOperation } from '@comunica/context-entries';
import { ActionContext, Bus } from '@comunica/core';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { getSafeBindings, getSafeBoolean, getSafeQuads } from '@comunica/utils-query-operation';
import { KeysBindings } from '@incremunica/context-entries';
import { createTestContextWithDataFactory, quad } from '@incremunica/dev-tools';
import type { Quad } from '@incremunica/types';
import { arrayifyStream } from 'arrayify-stream';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ActorQueryOperationSlice } from '../lib';
import '@comunica/utils-jest';
import '@incremunica/jest';

const DF = new DataFactory();
const BF = new BindingsFactory(DF);

describe('ActorQueryOperationSlice', () => {
  let bus: any;
  let mediatorQueryOperation: any;
  let mediatorQueryOperationMetaInf: any;
  let mediatorQueryOperationUndefs: any;
  let mediatorQueryOperationQuads: any;
  let mediatorQueryOperationBoolean: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    mediatorQueryOperation = {
      mediate: jest.fn((arg: any) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
        ], { autoStart: false }),
        metadata: () => Promise.resolve({
          cardinality: { type: 'estimate', value: 3 },

          variables: [{ variable: DF.variable('a'), canBeUndef: false }],
        }),
        operated: arg,
        type: 'bindings',
      })),
    };
    mediatorQueryOperationMetaInf = {
      mediate: (arg: any) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
        ], { autoStart: false }),
        metadata: () => Promise.resolve({
          cardinality: { type: 'estimate', value: Number.POSITIVE_INFINITY },

          variables: [{ variable: DF.variable('a'), canBeUndef: false }],
        }),
        operated: arg,
        type: 'bindings',
      }),
    };
    mediatorQueryOperationUndefs = {
      mediate: (arg: any) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
        ], { autoStart: false }),
        metadata: () => Promise.resolve({
          cardinality: { type: 'estimate', value: 3 },
          variables: [{ variable: DF.variable('a'), canBeUndef: true }],
        }),
        operated: arg,
        type: 'bindings',
      }),
    };
    mediatorQueryOperationQuads = {
      mediate: (arg: any) => Promise.resolve({
        quadStream: new ArrayIterator([
          DF.quad(DF.namedNode('http://example.com/s'), DF.namedNode('http://example.com/p'), DF.literal('1')),
          DF.quad(DF.namedNode('http://example.com/s'), DF.namedNode('http://example.com/p'), DF.literal('2')),
          DF.quad(DF.namedNode('http://example.com/s'), DF.namedNode('http://example.com/p'), DF.literal('3')),
        ], { autoStart: false }),
        metadata: () => Promise.resolve({ cardinality: { type: 'estimate', value: 3 }}),
        operated: arg,
        type: 'quads',
      }),
    };
    mediatorQueryOperationBoolean = {
      mediate: () => Promise.resolve({
        execute: async() => true,
        type: 'boolean',
      }),
    };
  });

  describe('The ActorQueryOperationSlice module', () => {
    it('should be a function', () => {
      expect(ActorQueryOperationSlice).toBeInstanceOf(Function);
    });

    it('should be a ActorQueryOperationSlice constructor', () => {
      expect(new (<any> ActorQueryOperationSlice)({ name: 'actor', bus })).toBeInstanceOf(ActorQueryOperationSlice);
      expect(new (<any> ActorQueryOperationSlice)({ name: 'actor', bus })).toBeInstanceOf(ActorQueryOperation);
    });

    it('should not be able to create new ActorQueryOperationSlice objects without \'new\'', () => {
      expect(() => {
        (<any> ActorQueryOperationSlice)();
      }).toThrow(`Class constructor ActorQueryOperationSlice cannot be invoked without 'new'`);
    });
  });

  describe('An ActorQueryOperationSlice instance', () => {
    let actor: ActorQueryOperationSlice;

    beforeEach(() => {
      actor = new ActorQueryOperationSlice({ name: 'actor', bus, mediatorQueryOperation });
    });

    it('should test on slices', async() => {
      const op: any = { operation: { type: 'slice', start: 0, length: 100 }, context: new ActionContext() };
      await expect(actor.test(op)).resolves.toPassTestVoid();
    });

    it('should not test on non-slices', async() => {
      const op: any = { operation: { type: 'no-slice' }};
      await expect(actor.test(op)).resolves.toFailTest(`Actor actor only supports slice operations, but got no-slice`);
    });

    it('should run on a stream for start 0 and length 100', async() => {
      const op: any = { operation: { type: 'project', start: 0, length: 100 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 3 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      expect(mediatorQueryOperation.mediate.mock.calls[0][0].context.get(KeysQueryOperation.limitIndicator))
        .toBe(100);
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
      ]);
    });

    it('should run on a stream for start 1 and length 100', async() => {
      const op: any = { operation: { type: 'project', start: 1, length: 100 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 2 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
      ]);
    });

    it('should run on a stream for start 3 and length 100', async() => {
      const op: any = { operation: { type: 'project', start: 3, length: 100 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 0 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([]);
    });

    it('should run on a stream for start 0 and length 3', async() => {
      const op: any = { operation: { type: 'project', start: 0, length: 3 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 3 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
      ]);
    });

    it('should run on a stream for start 0 and length 2', async() => {
      const op: any = { operation: { type: 'project', start: 0, length: 2 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 2 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
      ]);
    });

    it('should run on a stream for start 0 and length 0', async() => {
      const op: any = { operation: { type: 'project', start: 0, length: 0 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 0 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(mediatorQueryOperation.mediate.mock.calls[0][0].context.get(KeysQueryOperation.limitIndicator))
        .toBeUndefined();
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([]);
    });

    it('should run on a stream for start 1 and length 3', async() => {
      const op: any = { operation: { type: 'project', start: 1, length: 3 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 2 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
      ]);
    });

    it('should run on a stream for start 1 and length 1', async() => {
      const op: any = { operation: { type: 'project', start: 1, length: 1 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 1 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
      ]);
    });

    it('should run on a stream for start 2 and length 1', async() => {
      const op: any = { operation: { type: 'project', start: 2, length: 1 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 1 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
      ]);
    });

    it('should run on a stream for start 2 and length 0', async() => {
      const op: any = { operation: { type: 'project', start: 2, length: 0 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 0 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([]);
    });

    it('should run on a stream for start 3 and length 1', async() => {
      const op: any = { operation: { type: 'project', start: 3, length: 1 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 0 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      await expect(output.bindingsStream).toEqualBindingsStream([]);
    });

    it('should run on a stream for start 3 and length 0', async() => {
      const op: any = { operation: { type: 'project', start: 3, length: 1 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 0 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([]);
    });

    it('should run on a stream for start 4 and length 1', async() => {
      const op: any = { operation: { type: 'project', start: 4, length: 1 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 0 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([]);
    });

    it('should run on a stream for start 4 and length 0', async() => {
      const op: any = { operation: { type: 'project', start: 4, length: 1 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 0 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([]);
    });

    it(`should run on a stream for start 0 and length 100 when the mediator provides metadata with infinity`, async() => {
      actor = new ActorQueryOperationSlice({
        bus,
        mediatorQueryOperation: mediatorQueryOperationMetaInf,
        name: 'actor',
      });
      const op: any = { operation: { type: 'project', start: 0, length: 100 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: Number.POSITIVE_INFINITY },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
      ]);
    });

    it('should run on a stream for start 0 and length 100 when the mediator provides undefs', async() => {
      actor = new ActorQueryOperationSlice({
        bus,
        mediatorQueryOperation: mediatorQueryOperationUndefs,
        name: 'actor',
      });
      const op: any = { operation: { type: 'project', start: 0, length: 100 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 3 },
        variables: [{ variable: DF.variable('a'), canBeUndef: true }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
      ]);
    });

    it('should run on a stream for start 0 and no length', async() => {
      const op: any = { operation: { type: 'project', start: 0 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 3 },

        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('1') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
      ]);
    });

    it('should run on a stream of quads for start 0 and length 2', async() => {
      actor = new ActorQueryOperationSlice({ bus, mediatorQueryOperation: mediatorQueryOperationQuads, name: 'actor' });
      const op: any = { operation: { type: 'project', start: 0, length: 2 }, context: new ActionContext() };
      const output = getSafeQuads(await actor.run(op, undefined));
      await expect(output.metadata()).resolves
        .toEqual({ cardinality: { type: 'estimate', value: 2 }});
      expect(output.type).toBe('quads');
      await expect(arrayifyStream(output.quadStream)).resolves.toEqual([
        DF.quad(DF.namedNode('http://example.com/s'), DF.namedNode('http://example.com/p'), DF.literal('1')),
        DF.quad(DF.namedNode('http://example.com/s'), DF.namedNode('http://example.com/p'), DF.literal('2')),
      ]);
    });

    it('should return the output as-is if the output is neither quads nor bindings', async() => {
      actor = new ActorQueryOperationSlice({
        bus,
        mediatorQueryOperation: mediatorQueryOperationBoolean,
        name: 'actor',
      });
      const op: any = { operation: { type: 'project', start: 0 }, context: new ActionContext() };
      const output = getSafeBoolean(await actor.run(op, undefined));
      expect(output.type).toBe('boolean');
      await expect(output.execute()).resolves.toBe(true);
    });

    describe('testing deletions', () => {
      beforeEach(() => {
        jest.spyOn(mediatorQueryOperation, 'mediate').mockImplementation((arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, false),
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, false),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },
            variables: [{ variable: DF.variable('a'), canBeUndef: false }],
          }),
          operated: arg,
          type: 'bindings',
        }));
        const quad1Del = <Quad>DF.quad(DF.namedNode('http://example.com/s'), DF.namedNode('http://example.com/p'), DF.literal('1'));
        const quad2Del = <Quad>DF.quad(DF.namedNode('http://example.com/s'), DF.namedNode('http://example.com/p'), DF.literal('2'));
        quad1Del.isAddition = false;
        quad2Del.isAddition = false;
        mediatorQueryOperationQuads.mediate = (arg: any) => Promise.resolve({
          quadStream: new ArrayIterator([
            DF.quad(DF.namedNode('http://example.com/s'), DF.namedNode('http://example.com/p'), DF.literal('1')),
            DF.quad(DF.namedNode('http://example.com/s'), DF.namedNode('http://example.com/p'), DF.literal('2')),
            DF.quad(DF.namedNode('http://example.com/s'), DF.namedNode('http://example.com/p'), DF.literal('3')),
            quad2Del,
            quad1Del,
          ], { autoStart: false }),
          metadata: () => Promise.resolve({ cardinality: { type: 'estimate', value: 3 }}),
          operated: arg,
          type: 'quads',
        });
      });

      it('should fail on a non-existing deletion', async() => {
        jest.spyOn(mediatorQueryOperation, 'mediate').mockImplementation((arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, false),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 1 },
            variables: [{ variable: DF.variable('a'), canBeUndef: false }],
          }),
          operated: arg,
          type: 'bindings',
        }));
        const op: any = { operation: { type: 'project', start: 0, length: 1 }, context: new ActionContext() };
        const output = getSafeBindings(await actor.run(op, undefined));
        await expect(new Promise<void>((resolve, reject) => {
          output.bindingsStream.on('data', () => {
            resolve();
          });
          output.bindingsStream.on('end', () => {
            resolve();
          });
          output.bindingsStream.on('error', (e) => {
            reject(e);
          });
        })).rejects.toThrow('Deletion {\n  "a": "\\"1\\""\n}, has not been added.');
      });

      it('should run on a stream for start 0 and length 1', async() => {
        const op: any = { operation: { type: 'project', start: 0, length: 1 }, context: new ActionContext() };
        const output = getSafeBindings(await actor.run(op, undefined));
        await expect(output.metadata()).resolves.toEqual({
          cardinality: { type: 'estimate', value: 1 },

          variables: [{ variable: DF.variable('a'), canBeUndef: false }],
        });
        expect(output.type).toBe('bindings');
        await expect(output.bindingsStream).toEqualBindingsStream([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.isAddition, true),
        ]);
      });

      it('should run on a stream for start 1 and length 1', async() => {
        const op: any = { operation: { type: 'project', start: 1, length: 1 }, context: new ActionContext() };
        const output = getSafeBindings(await actor.run(op, undefined));
        await expect(output.metadata()).resolves.toEqual({
          cardinality: { type: 'estimate', value: 1 },

          variables: [{ variable: DF.variable('a'), canBeUndef: false }],
        });
        expect(output.type).toBe('bindings');
        await expect(output.bindingsStream).toEqualBindingsStream([
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, false),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, false),
        ]);
      });

      it('should run on a stream for start 2 and length 1', async() => {
        const op: any = { operation: { type: 'project', start: 2, length: 1 }, context: new ActionContext() };
        const output = getSafeBindings(await actor.run(op, undefined));
        await expect(output.metadata()).resolves.toEqual({
          cardinality: { type: 'estimate', value: 1 },

          variables: [{ variable: DF.variable('a'), canBeUndef: false }],
        });
        expect(output.type).toBe('bindings');
        await expect(output.bindingsStream).toEqualBindingsStream([
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.isAddition, false),
        ]);
      });

      it('should run on a stream for start 3 and length 1', async() => {
        const op: any = { operation: { type: 'project', start: 3, length: 1 }, context: new ActionContext() };
        const output = getSafeBindings(await actor.run(op, undefined));
        await expect(output.metadata()).resolves.toEqual({
          cardinality: { type: 'estimate', value: 0 },

          variables: [{ variable: DF.variable('a'), canBeUndef: false }],
        });
        expect(output.type).toBe('bindings');
        await expect(output.bindingsStream).toEqualBindingsStream([]);
      });

      it('should run on a stream for start 4 and length 1', async() => {
        const op: any = { operation: { type: 'project', start: 4, length: 1 }, context: new ActionContext() };
        const output = getSafeBindings(await actor.run(op, undefined));
        await expect(output.metadata()).resolves.toEqual({
          cardinality: { type: 'estimate', value: 0 },

          variables: [{ variable: DF.variable('a'), canBeUndef: false }],
        });
        expect(output.type).toBe('bindings');
        await expect(output.bindingsStream).toEqualBindingsStream([]);
      });
    });

    describe('testing bag semantics', () => {
      beforeEach(() => {
        jest.spyOn(mediatorQueryOperation, 'mediate').mockImplementation((arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, true),
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, false),
            BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, false),
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, false),
            BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, false),
            BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.isAddition, false),
            BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.isAddition, false),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },

            variables: [{ variable: DF.variable('a'), canBeUndef: false }],
          }),
          operated: arg,
          type: 'bindings',
        }));

        mediatorQueryOperationQuads.mediate = (arg: any) => Promise.resolve({
          quadStream: new ArrayIterator([
            quad('http://example.com/s', 'http://example.com/p', '1', 'http://example.com/g', true),
            quad('http://example.com/s', 'http://example.com/p', '2', 'http://example.com/g', true),
            quad('http://example.com/s', 'http://example.com/p', '1', 'http://example.com/g', true),
            quad('http://example.com/s', 'http://example.com/p', '2', 'http://example.com/g', true),
            quad('http://example.com/s', 'http://example.com/p', '1', 'http://example.com/g', true),
            quad('http://example.com/s', 'http://example.com/p', '2', 'http://example.com/g', true),
            quad('http://example.com/s', 'http://example.com/p', '1', 'http://example.com/g', false),
            quad('http://example.com/s', 'http://example.com/p', '2', 'http://example.com/g', false),
            quad('http://example.com/s', 'http://example.com/p', '1', 'http://example.com/g', false),
            quad('http://example.com/s', 'http://example.com/p', '2', 'http://example.com/g', false),
            quad('http://example.com/s', 'http://example.com/p', '1', 'http://example.com/g', false),
            quad('http://example.com/s', 'http://example.com/p', '2', 'http://example.com/g', false),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({ cardinality: { type: 'estimate', value: 3 }}),
          operated: arg,
          type: 'quads',
        });
      });

      const testCases = [
        { start: 0, length: 1, result: [[ '1', true ], [ '1', false ], [ '2', true ], [ '2', false ]]},
        { start: 0, length: 2, result: [[ '1', true ], [ '2', true ], [ '1', false ], [ '2', false ]]},
        { start: 0, length: 3, result: [
          [ '1', true ],
          [ '2', true ],
          [ '1', true ],
          [ '1', false ],
          [ '2', true ],
          [ '2', false ],
          [ '1', false ],
          [ '2', false ],
        ]},
        { start: 0, length: 4, result: [
          [ '1', true ],
          [ '2', true ],
          [ '1', true ],
          [ '2', true ],
          [ '1', false ],
          [ '2', false ],
          [ '1', false ],
          [ '2', false ],
        ]},
        { start: 2, length: 1, result: [[ '1', true ], [ '1', false ]]},
        { start: 2, length: 2, result: [[ '1', true ], [ '2', true ], [ '1', false ], [ '2', false ]]},
        { start: 2, length: 3, result: [
          [ '1', true ],
          [ '2', true ],
          [ '1', true ],
          [ '1', false ],
          [ '1', false ],
          [ '2', false ],
        ]},
        { start: 2, length: 4, result: [
          [ '1', true ],
          [ '2', true ],
          [ '1', true ],
          [ '2', true ],
          [ '1', false ],
          [ '1', false ],
          [ '2', false ],
          [ '2', false ],
        ]},
      ];

      it.each(testCases)(
        'should run on a bindings stream with start %s',
        async({ start, length, result }) => {
          const op: any = { operation: { type: 'project', start, length }, context: new ActionContext() };
          const output = getSafeBindings(await actor.run(op, undefined));
          expect(output.type).toBe('bindings');
          const expectedResults = result.map((value: [string, boolean]) =>
            BF.bindings([
              [ DF.variable('a'), DF.literal(value[0]) ],
            ]).setContextEntry(KeysBindings.isAddition, value[1]));
          await expect(output.bindingsStream).toEqualBindingsStream(expectedResults);
        },
      );

      it.each(testCases)(
        'should run on a quad stream with start %s',
        async({ start, length, result }) => {
          actor = new ActorQueryOperationSlice({
            bus,
            mediatorQueryOperation: mediatorQueryOperationQuads,
            name: 'actor',
          });
          const op: any = {
            operation: { type: 'project', start, length },
            context: createTestContextWithDataFactory(),
          };
          const output = getSafeQuads(await actor.run(op, undefined));
          expect(output.type).toBe('quads');
          const expectedResults = result.map((value: [string, boolean]) =>
            quad('http://example.com/s', 'http://example.com/p', value[0], 'http://example.com/g', value[1]));
          await expect(arrayifyStream(output.quadStream)).resolves.toEqual(expectedResults);
        },
      );
    });
  });
});
