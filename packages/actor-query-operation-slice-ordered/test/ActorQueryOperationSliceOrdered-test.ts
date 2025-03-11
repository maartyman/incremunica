import { ActorQueryOperation } from '@comunica/bus-query-operation';
import { KeysQueryOperation } from '@comunica/context-entries';
import { ActionContext, Bus } from '@comunica/core';
import { BindingsFactory } from '@comunica/utils-bindings-factory';
import { getSafeBindings } from '@comunica/utils-query-operation';
import { KeysBindings } from '@incremunica/context-entries';
import { ArrayIterator } from 'asynciterator';
import { DataFactory } from 'rdf-data-factory';
import { ActorQueryOperationSliceOrdered } from '../lib';
import '@comunica/utils-jest';
import '@incremunica/jest';

const DF = new DataFactory();
const BF = new BindingsFactory(DF);

describe('ActorQueryOperationSliceOrdered', () => {
  let bus: any;
  let mediatorQueryOperation: any;
  let mediatorQueryOperationMetaInf: any;
  let mediatorQueryOperationUndefs: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    mediatorQueryOperation = {
      mediate: jest.fn((arg: any) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.order, { index: 0 }),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.order, { index: 1 }),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.order, { index: 2 }),
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
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.order, { index: 0 }),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.order, { index: 1 }),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.order, { index: 2 }),
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
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.order, { index: 0 }),
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.order, { index: 1 }),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.order, { index: 2 }),
        ], { autoStart: false }),
        metadata: () => Promise.resolve({
          cardinality: { type: 'estimate', value: 3 },
          variables: [{ variable: DF.variable('a'), canBeUndef: true }],
        }),
        operated: arg,
        type: 'bindings',
      }),
    };
  });

  describe('The ActorQueryOperationSliceOrdered module', () => {
    it('should be a function', () => {
      expect(ActorQueryOperationSliceOrdered).toBeInstanceOf(Function);
    });

    it('should be a ActorQueryOperationSliceOrdered constructor', () => {
      expect(new (<any> ActorQueryOperationSliceOrdered)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorQueryOperationSliceOrdered);
      expect(new (<any> ActorQueryOperationSliceOrdered)({ name: 'actor', bus }))
        .toBeInstanceOf(ActorQueryOperation);
    });

    it('should not be able to create new ActorQueryOperationSliceOrdered objects without \'new\'', () => {
      expect(() => {
        (<any> ActorQueryOperationSliceOrdered)();
      }).toThrow(`Class constructor ActorQueryOperationSliceOrdered cannot be invoked without 'new'`);
    });
  });

  describe('An ActorQueryOperationSliceOrdered instance', () => {
    let actor: ActorQueryOperationSliceOrdered;

    beforeEach(() => {
      actor = new ActorQueryOperationSliceOrdered({ name: 'actor', bus, mediatorQueryOperation });
    });

    it('should fail on only slices', async() => {
      const op: any = { operation: { type: 'slice', start: 0, length: 100 }, context: new ActionContext() };
      await expect(actor.test(op)).resolves.toFailTest('This actor can only handle slices after an order operation.');
    });

    it('should fail on only slices and a projection', async() => {
      const op: any = {
        operation: { type: 'slice', start: 0, length: 100, input: { type: 'project' }},
        context: new ActionContext(),
      };
      await expect(actor.test(op)).resolves.toFailTest('This actor can only handle slices after an order operation.');
    });

    it('should test on slices with an orderby', async() => {
      const op: any = {
        operation: { type: 'slice', start: 0, length: 100, input: { type: 'orderby' }},
        context: new ActionContext(),
      };
      await expect(actor.test(op)).resolves.toPassTestVoid();
    });

    it('should test on slices with a projection and then orderby', async() => {
      const op: any = {
        operation: { type: 'slice', start: 0, length: 100, input: { type: 'project', input: { type: 'orderby' }}},
        context: new ActionContext(),
      };
      await expect(actor.test(op)).resolves.toPassTestVoid();
    });

    it('should fail on slices with a projection and then a bgp', async() => {
      const op: any = {
        operation: { type: 'slice', start: 0, length: 100, input: { type: 'project', input: { type: 'bgp' }}},
        context: new ActionContext(),
      };
      await expect(actor.test(op)).resolves.toFailTest(`This actor can only handle slices after an order operation.`);
    });

    it('should not test on non-slices', async() => {
      const op: any = { operation: { type: 'no-slice' }, context: new ActionContext() };
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
      actor = new ActorQueryOperationSliceOrdered({
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
      actor = new ActorQueryOperationSliceOrdered({
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

    it('should run on a stream for start 1 and length 1 (overflow)', async() => {
      jest.spyOn(mediatorQueryOperation, 'mediate').mockImplementation((arg: any) => Promise.resolve({
        bindingsStream: new ArrayIterator([
          BF.bindings([[ DF.variable('a'), DF.literal('2') ]]).setContextEntry(KeysBindings.order, { index: 0 }),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.order, { index: 1 }),
          BF.bindings([[ DF.variable('a'), DF.literal('1') ]]).setContextEntry(KeysBindings.order, { index: 0 }),
        ], { autoStart: false }),
        metadata: () => Promise.resolve({
          cardinality: { type: 'estimate', value: 3 },

          variables: [{ variable: DF.variable('a'), canBeUndef: false }],
        }),
        operated: arg,
        type: 'bindings',
      }));
      const op: any = { operation: { type: 'project', start: 1, length: 1 }, context: new ActionContext() };
      const output = getSafeBindings(await actor.run(op, undefined));
      await expect(output.metadata()).resolves.toEqual({
        cardinality: { type: 'estimate', value: 1 },
        variables: [{ variable: DF.variable('a'), canBeUndef: false }],
      });
      expect(output.type).toBe('bindings');
      await expect(output.bindingsStream).toEqualBindingsStream([
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]),
        BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.isAddition, false),
        BF.bindings([[ DF.variable('a'), DF.literal('2') ]]),
      ]);
    });

    describe('testing deletions', () => {
      beforeEach(() => {
        jest.spyOn(mediatorQueryOperation, 'mediate').mockImplementation((arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('1') ],
            ]).setContextEntry(KeysBindings.isAddition, true).setContextEntry(KeysBindings.order, { index: 0 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2') ],
            ]).setContextEntry(KeysBindings.isAddition, true).setContextEntry(KeysBindings.order, { index: 1 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('3') ],
            ]).setContextEntry(KeysBindings.isAddition, true).setContextEntry(KeysBindings.order, { index: 2 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2') ],
            ]).setContextEntry(KeysBindings.isAddition, false).setContextEntry(KeysBindings.order, { index: 1 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('1') ],
            ]).setContextEntry(KeysBindings.isAddition, false).setContextEntry(KeysBindings.order, { index: 0 }),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },
            variables: [{ variable: DF.variable('a'), canBeUndef: false }],
          }),
          operated: arg,
          type: 'bindings',
        }));
      });

      it('should fail on a non-existing deletion', async() => {
        jest.spyOn(mediatorQueryOperation, 'mediate').mockImplementation((arg: any) => Promise.resolve({
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.literal('1') ],
            ]).setContextEntry(KeysBindings.isAddition, false),
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
        })).rejects.toThrow('Missing order context on bindings: {\n  "a": "\\"1\\""\n}');
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
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.isAddition, true),
          BF.bindings([[ DF.variable('a'), DF.literal('3') ]]).setContextEntry(KeysBindings.isAddition, false),
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
            BF.bindings([
              [ DF.variable('a'), DF.literal('1') ],
            ]).setContextEntry(KeysBindings.isAddition, true).setContextEntry(KeysBindings.order, { index: 0 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2') ],
            ]).setContextEntry(KeysBindings.isAddition, true).setContextEntry(KeysBindings.order, { index: 1 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('1') ],
            ]).setContextEntry(KeysBindings.isAddition, true).setContextEntry(KeysBindings.order, { index: 1 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2') ],
            ]).setContextEntry(KeysBindings.isAddition, true).setContextEntry(KeysBindings.order, { index: 3 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('1') ],
            ]).setContextEntry(KeysBindings.isAddition, true).setContextEntry(KeysBindings.order, { index: 2 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2') ],
            ]).setContextEntry(KeysBindings.isAddition, true).setContextEntry(KeysBindings.order, { index: 5 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('1') ],
            ]).setContextEntry(KeysBindings.isAddition, false).setContextEntry(KeysBindings.order, { index: 2 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2') ],
            ]).setContextEntry(KeysBindings.isAddition, false).setContextEntry(KeysBindings.order, { index: 4 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('1') ],
            ]).setContextEntry(KeysBindings.isAddition, false).setContextEntry(KeysBindings.order, { index: 1 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2') ],
            ]).setContextEntry(KeysBindings.isAddition, false).setContextEntry(KeysBindings.order, { index: 2 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('1') ],
            ]).setContextEntry(KeysBindings.isAddition, false).setContextEntry(KeysBindings.order, { index: 0 }),
            BF.bindings([
              [ DF.variable('a'), DF.literal('2') ],
            ]).setContextEntry(KeysBindings.isAddition, false).setContextEntry(KeysBindings.order, { index: 0 }),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },

            variables: [{ variable: DF.variable('a'), canBeUndef: false }],
          }),
          operated: arg,
          type: 'bindings',
        }));
      });

      const testCases: { start: number; length: number; result: [string, boolean][] }[] = [
        { start: 0, length: 1, result: [[ '1', true ], [ '1', false ], [ '2', true ], [ '2', false ]]},
        { start: 0, length: 2, result: [
          [ '1', true ],
          [ '2', true ],
          [ '2', false ],
          [ '1', true ],
          [ '1', false ],
          [ '2', true ],
          [ '1', false ],
          [ '2', false ],
        ]},
        { start: 0, length: 3, result: [
          [ '1', true ],
          [ '2', true ],
          [ '1', true ],
          [ '2', false ],
          [ '1', true ],
          [ '1', false ],
          [ '2', true ],
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
          [ '2', false ],
          [ '1', true ],
          [ '1', false ],
          [ '2', true ],
          [ '1', false ],
          [ '2', false ],
          [ '1', false ],
          [ '2', false ],
        ]},
        { start: 2, length: 1, result: [
          [ '2', true ],
          [ '2', false ],
          [ '1', true ],
          [ '1', false ],
          [ '2', true ],
          [ '2', false ],
          [ '2', true ],
          [ '2', false ],
        ]},
        { start: 2, length: 2, result: [
          [ '2', true ],
          [ '2', true ],
          [ '2', false ],
          [ '1', true ],
          [ '1', false ],
          [ '2', true ],
          [ '2', false ],
          [ '2', false ],
        ]},
        { start: 2, length: 3, result: [
          [ '2', true ],
          [ '2', true ],
          [ '1', true ],
          [ '1', false ],
          [ '2', true ],
          [ '2', false ],
          [ '2', false ],
          [ '2', false ],
        ]},
        { start: 2, length: 4, result: [
          [ '2', true ],
          [ '2', true ],
          [ '1', true ],
          [ '2', true ],
          [ '1', false ],
          [ '2', false ],
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
    });
  });
});
