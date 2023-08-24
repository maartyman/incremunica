import '@comunica/incremental-jest';
import {IActionContext, IJoinEntry, IJoinEntryWithMetadata, IQueryOperationResultBindings} from "@comunica/types";
import {ActionContext, Actor, Bus, IActorTest, Mediator} from "@comunica/core";
import {IActionRdfJoinSelectivity, IActorRdfJoinSelectivityOutput} from "@comunica/bus-rdf-join-selectivity";
import {IActionQueryOperation, MediatorQueryOperation} from "@comunica/bus-query-operation";
import {ArrayIterator, CLOSING, EmptyIterator, WrappingIterator} from "asynciterator";
import {DataFactory} from "rdf-data-factory";
import {BindingsFactory, bindingsToString} from "@comunica/incremental-bindings-factory";
import {Factory} from "sparqlalgebrajs";
import {DeltaQueryIterator} from "../lib/DeltaQueryIterator";
import {Bindings, BindingsStream} from "@comunica/incremental-types";
import {getContextSources} from "@comunica/bus-rdf-resolve-quad-pattern";
import {Store} from "n3";
import arrayifyStream from "arrayify-stream";
import {promisifyEventEmitter} from "event-emitter-promisify/dist";
import * as RDF from "@rdfjs/types";
import {Transform} from "readable-stream";
import EventEmitter = require("events");
import {MetadataValidationState} from "@comunica/metadata";
const streamifyArray = require('streamify-array');

const DF = new DataFactory();
const BF = new BindingsFactory();
const FACTORY = new Factory();

async function partialArrayifyStream(stream: EventEmitter, num: number): Promise<any[]> {
  let array: any[] = [];
  for (let i = 0; i < num; i++) {
    await new Promise<void>((resolve) => stream.once("data", (bindings: any) => {
      array.push(bindings);
      resolve();
    }));
  }
  return array;
}

function nullifyVariables(term?: RDF.Term): RDF.Term | undefined {
  return !term || term.termType === 'Variable' ? undefined : term;
}

describe("DeltaQueryIterator", () => {
  let bus: any;
  let context: IActionContext;
  let mediatorQueryOperation: MediatorQueryOperation;
  let mediateFunc: jest.Mock;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
    context = new ActionContext();

    mediateFunc = jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
      const sources = getContextSources(arg.context);

      expect(sources).not.toBeUndefined();
      if (sources === undefined) {
        return {
          bindingsStream: new EmptyIterator(),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 0 },
            canContainUndefs: false,
            variables: [ DF.variable('a'), DF.variable('a')],
            state: new MetadataValidationState()
          }),
          type: 'bindings',
        }
      }

      const bindingstream: BindingsStream = new ArrayIterator((<Store>sources[0]).match(
        // @ts-ignore
        nullifyVariables(arg.operation.subject),
        nullifyVariables(arg.operation.predicate),
        nullifyVariables(arg.operation.object),
        null,
      )).map((quad) => {
        if (quad.predicate.value === "ex:p2") {
          return BF.bindings([
          ]);
        } else {
          return BF.bindings([
            [ DF.variable('b'), quad.object ],
          ]);
        }
      })

      return {
        bindingsStream: bindingstream,
        metadata: () => Promise.resolve({
          cardinality: { type: 'estimate', value: 1 },
          canContainUndefs: false,
          variables: [ DF.variable('bound') ],
          state: new MetadataValidationState()
        }),
        type: 'bindings',
      };
    });

    mediatorQueryOperation = <any> {
      mediate: mediateFunc
    };
  });

  it('should join two entries', async() => {
    const action: IJoinEntry[] = [
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
              [ DF.variable('b'), DF.namedNode('ex:b2') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },
            canContainUndefs: false,
            variables: [ DF.variable('a'), DF.variable('b') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 1 },
            canContainUndefs: false,
            variables: [ DF.variable('a') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];


    const delta = new DeltaQueryIterator(
      action,
      context,
      mediatorQueryOperation
    );

    expect(await arrayifyStream(delta)).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]),BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ]),
    ]);

    expect(mediateFunc).toBeCalledTimes(4);
  });

  it('should join two entries with deletions', async() => {
    const action: IJoinEntry[] = [
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
              [ DF.variable('b'), DF.namedNode('ex:b2') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ], false),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },
            canContainUndefs: false,
            variables: [ DF.variable('a'), DF.variable('b') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a2') ],
            ], false),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 1 },
            canContainUndefs: false,
            variables: [ DF.variable('a') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];


    const delta = new DeltaQueryIterator(
      action,
      context,
      mediatorQueryOperation
    );

    expect(await arrayifyStream(delta)).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ], false),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ], false),
    ]);

    expect(mediateFunc).toBeCalledTimes(6);
  });

  it('should join two slow entries', async() => {
    const transform = new Transform({
      transform(quad: any, _encoding: any, callback: (arg0: null, arg1: any) => any) {
        return callback(null, quad);
      },
      objectMode: true,
    });

    let stream = streamifyArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ])
    ], { autoStart: false }).pipe(transform, {end: false});

    let it1 = new WrappingIterator(stream);

    let it2 = new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
      ]),
    ], { autoStart: false });

    const action: IJoinEntry[] = [
      {
        output: <any> {
          bindingsStream: it1,
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },
            canContainUndefs: false,
            variables: [ DF.variable('a'), DF.variable('b') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any> {
          bindingsStream: it2,
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 1 },
            canContainUndefs: false,
            variables: [ DF.variable('a') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      context,
      mediatorQueryOperation
    );

    expect(await partialArrayifyStream(delta, 1)).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ])
    ]);

    stream.push(
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ])
    );
    stream.end();

    expect(await arrayifyStream(delta)).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ]),
    ]);

    expect(mediateFunc).toBeCalledTimes(4);
  });

  it('should keep reading the bindingsStream if the bindings can\'t be bound to the quad', async() => {
    const action: IJoinEntry[] = [
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },
            canContainUndefs: false,
            variables: [ DF.variable('a'), DF.variable('b') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('c'), DF.namedNode('ex:a1') ],
            ]),
            BF.bindings([
              [ DF.variable('d'), DF.namedNode('ex:a1') ],
            ]),
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 1 },
            canContainUndefs: false,
            variables: [ DF.variable('a') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];


    const delta = new DeltaQueryIterator(
      action,
      context,
      mediatorQueryOperation
    );

    expect(await arrayifyStream(delta)).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ])
    ]);

    expect(mediateFunc).toBeCalledTimes(2);
  });

  it('should keep reading the bindingsSteam and break if it has a null', async() => {
    const action: IJoinEntry[] = [
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },
            canContainUndefs: false,
            variables: [ DF.variable('a'), DF.variable('b') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('c'), DF.namedNode('ex:a1') ],
            ]),
            null,
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 1 },
            canContainUndefs: false,
            variables: [ DF.variable('a') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];


    const delta = new DeltaQueryIterator(
      action,
      context,
      mediatorQueryOperation
    );

    expect(await arrayifyStream(delta)).toBeIsomorphicBindingsArray([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ])
    ]);

    expect(mediateFunc).toBeCalledTimes(2);
  });

  it('should destroy entries on end', async() => {
    let it1 = new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ]),
    ], { autoStart: false });

    let it2 = new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
      ]),
    ], { autoStart: false });

    const action: IJoinEntry[] = [
      {
        output: <any> {
          bindingsStream: it1,
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },
            canContainUndefs: false,
            variables: [ DF.variable('a'), DF.variable('b') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any> {
          bindingsStream: it2,
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 1 },
            canContainUndefs: false,
            variables: [ DF.variable('a') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      context,
      mediatorQueryOperation
    );

    delta.close();

    await new Promise<void>((resolve)=>setTimeout(()=>resolve(),100))

    expect(it1.destroyed).toBeTruthy();
    expect(it2.destroyed).toBeTruthy();
  });

  it('should handle destroyed entry', async() => {
    let it1 = new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
        [ DF.variable('b'), DF.namedNode('ex:b1') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
        [ DF.variable('b'), DF.namedNode('ex:b2') ],
      ]),
    ], { autoStart: false });

    let it2 = new ArrayIterator([
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a1') ],
      ]),
      BF.bindings([
        [ DF.variable('a'), DF.namedNode('ex:a2') ],
      ]),
    ], { autoStart: false });

    const action: IJoinEntry[] = [
      {
        output: <any> {
          bindingsStream: it1,
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },
            canContainUndefs: false,
            variables: [ DF.variable('a'), DF.variable('b') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any> {
          bindingsStream: it2,
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 1 },
            canContainUndefs: false,
            variables: [ DF.variable('a') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];

    const delta = new DeltaQueryIterator(
      action,
      context,
      mediatorQueryOperation
    );

    expect(() => {
      it1.destroy(new Error("test"))
    }).toThrow("test")
  });

  it('should internally return null when the bindings can\'t be merged', async() => {
    let mediateFunc = jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
      return {
        bindingsStream: new ArrayIterator([
          BF.bindings([
            [ DF.variable('a'), DF.namedNode('ex:a2') ],
          ]),
        ]),
        metadata: () => Promise.resolve({
          cardinality: { type: 'estimate', value: 1 },
          canContainUndefs: false,
          variables: [ DF.variable('bound') ],
          state: new MetadataValidationState()
        }),
        type: 'bindings',
      };
    });

    mediatorQueryOperation = <any> {
      mediate: mediateFunc
    };

    const action: IJoinEntry[] = [
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },
            canContainUndefs: false,
            variables: [ DF.variable('a'), DF.variable('b') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 1 },
            canContainUndefs: false,
            variables: [ DF.variable('a') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
    ];


    const delta = new DeltaQueryIterator(
      action,
      context,
      mediatorQueryOperation
    );

    expect(await arrayifyStream(delta)).toBeIsomorphicBindingsArray([]);

    expect(mediateFunc).toBeCalledTimes(2);
  });

  it('should join three entries', async() => {
    mediateFunc = jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
      return {
        bindingsStream: new EmptyIterator(),
        metadata: () => Promise.resolve({
          cardinality: { type: 'estimate', value: 1 },
          canContainUndefs: false,
          variables: [ DF.variable('bound') ],
          state: new MetadataValidationState()
        }),
        type: 'bindings',
      };
    });

    mediatorQueryOperation = <any> {
      mediate: mediateFunc
    };

    const action: IJoinEntry[] = [
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },
            canContainUndefs: false,
            variables: [ DF.variable('a'), DF.variable('b') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 1 },
            canContainUndefs: false,
            variables: [ DF.variable('a') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      },
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 1 },
            canContainUndefs: false,
            variables: [ DF.variable('a') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('b'), DF.namedNode('ex:p3'), DF.namedNode('ex:o2')),
      },
    ];


    const delta = new DeltaQueryIterator(
      action,
      context,
      mediatorQueryOperation
    );

    delta.on("data", () => {})

    await promisifyEventEmitter(delta);

    expect(mediateFunc).toBeCalledTimes(3);
  });

  it('should handle a failed sub query', async() => {
    mediateFunc = jest.fn(async(arg: IActionQueryOperation): Promise<IQueryOperationResultBindings> => {
      throw new Error("test")
    });

    mediatorQueryOperation = <any> {
      mediate: mediateFunc
    };

    const action: IJoinEntry[] = [
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
              [ DF.variable('b'), DF.namedNode('ex:b1') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 3 },
            canContainUndefs: false,
            variables: [ DF.variable('a'), DF.variable('b') ],
            state: new MetadataValidationState()
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p1'), DF.variable('b')),
      },
      {
        output: <any> {
          bindingsStream: new ArrayIterator([
            BF.bindings([
              [ DF.variable('a'), DF.namedNode('ex:a1') ],
            ]),
          ], { autoStart: false }),
          metadata: () => Promise.resolve({
            cardinality: { type: 'estimate', value: 1 },
            canContainUndefs: false,
            variables: [ DF.variable('a') ],
          }),
          type: 'bindings',
        },
        operation: FACTORY.createPattern(DF.variable('a'), DF.namedNode('ex:p2'), DF.namedNode('ex:o')),
      }
    ];


    const delta = new DeltaQueryIterator(
      action,
      context,
      mediatorQueryOperation
    );

    delta.on("data", () => {})

    await promisifyEventEmitter(delta);

    expect(mediateFunc).toBeCalledTimes(2);
  });
})
