import type { BindingsStream, ComunicaDataFactory, IActionContext } from '@comunica/types';
import type { Bindings } from '@comunica/utils-bindings-factory';
import type {
  IActorBindingsAggregatorFactoryOutput,
  MediatorBindingsAggregatorFactory,
} from '@incremunica/bus-bindings-aggregator-factory';
import { KeysBindings } from '@incremunica/context-entries';
import { AsyncIterator } from 'asynciterator';
import type * as RDF from 'rdf-js';
import type { Algebra } from 'sparqlalgebrajs';

type IGroupObject = {
  groupBindings: Bindings;
  aggregators: Record<string, IActorBindingsAggregatorFactoryOutput>;
  groupBuffer: Bindings[] | undefined;
  previousBindings: Bindings | undefined;
};

/**
 * A state manager for the groups constructed by consuming the bindings-stream.
 */
export class GroupIterator extends AsyncIterator<Bindings> {
  private readonly pattern: Algebra.Group;
  private readonly dataFactory: ComunicaDataFactory;
  private readonly groups: Map<number, IGroupObject>;
  private readonly groupVariables: Set<string>;
  private variablesInner: RDF.Variable[];
  private readonly hashFunction: (bindings: Bindings, variables: RDF.Variable[]) => number;
  private readonly context: IActionContext;
  private readonly mediatorBindingsAggregatorFactory: MediatorBindingsAggregatorFactory;
  private nextBindings: Bindings | null = null;

  public constructor(
    inputBindings: BindingsStream,
    context: IActionContext,
    pattern: Algebra.Group,
    dataFactory: ComunicaDataFactory,
    mediatorBindingsAggregatorFactory: MediatorBindingsAggregatorFactory,
    groupVariables: Set<string>,
    variablesInner: RDF.Variable[],
    hashFunction: (bindings: Bindings, variables: RDF.Variable[]) => number,
  ) {
    super();
    this.dataFactory = dataFactory;
    this.pattern = pattern;
    this.groups = new Map<number, IGroupObject>();
    this.variablesInner = variablesInner;
    this.groupVariables = groupVariables;
    this.hashFunction = hashFunction;
    this.context = context;
    this.mediatorBindingsAggregatorFactory = mediatorBindingsAggregatorFactory;

    // TODO [2024-11-21]: this is a bit of a hack, we should not be listening to the data event
    let dataCounter = 0;
    inputBindings.on('data', (bindings: Bindings) => {
      dataCounter++;
      this.putBindings(bindings)
        .then(() => {
          dataCounter--;
          if (dataCounter === 0) {
            this.readable = true;
          }
        })
        .catch(() => {
          dataCounter--;
          if (dataCounter === 0) {
            this.readable = true;
          }
        });
    });

    this.on('end', () => {
      this._cleanup();
    });
  }

  protected _cleanup(): void {
    this.groups.clear();
    this.groupVariables.clear();
    this.variablesInner = [];
  }

  public override _end(): void {
    super._end();
  }

  public override read(): Bindings | null {
    if (this.nextBindings) {
      const bindings = this.nextBindings;
      this.nextBindings = null;
      return bindings;
    }
    // TODO [2024-11-21]: maybe make sure this is done in round robin fashion
    for (const group of this.groups.values()) {
      if (group.groupBuffer) {
        continue;
      }
      let hasResult = false;
      let returnBindings = group.groupBindings;
      for (const variable in group.aggregators) {
        const value = group.aggregators[variable].result();
        if (value !== undefined) {
          hasResult = true;
        }
        if (value) {
          returnBindings = returnBindings.set(this.dataFactory.variable(variable), value);
        }
      }
      if (hasResult) {
        if (group.previousBindings === undefined) {
          // This will be the first result from this group, this will be an addition
          group.previousBindings = returnBindings;
          return returnBindings;
        }
        // This is a subsequent result from this group
        // first return the previous bindings as a deletion
        // then set the result as nextBindings
        this.nextBindings = returnBindings;
        returnBindings = group.previousBindings;
        returnBindings = returnBindings.setContextEntry(KeysBindings.isAddition, false);
        group.previousBindings = this.nextBindings;
        return returnBindings;
      }
    }
    this.readable = false;
    return null;
  }

  public async putBindings(bindings: Bindings): Promise<void> {
    const grouper = bindings
      .filter((_, variable) => this.groupVariables.has(variable.value));
    const groupHash = this.hashFunction(grouper, this.variablesInner);

    const group = this.groups.get(groupHash);
    if (group === undefined) {
      const initializeGroup: IGroupObject = {
        groupBindings: grouper,
        aggregators: {},
        groupBuffer: [ bindings ],
        previousBindings: undefined,
      };
      this.groups.set(groupHash, initializeGroup);
      await Promise.all(this.pattern.aggregates.map(async(aggregate) => {
        const key = aggregate.variable.value;
        initializeGroup.aggregators[key] = await this.mediatorBindingsAggregatorFactory
          .mediate({ expr: aggregate, context: this.context });
      }));
      const localGroupBuffer = initializeGroup.groupBuffer!;
      initializeGroup.groupBuffer = undefined;
      for (const bindings of localGroupBuffer) {
        for (const aggregate of this.pattern.aggregates) {
          const variable = aggregate.variable.value;
          await initializeGroup.aggregators[variable].putBindings(bindings);
        }
      }
      return;
    }
    if (group.groupBuffer) {
      group.groupBuffer.push(bindings);
      return;
    }
    for (const aggregate of this.pattern.aggregates) {
      const variable = aggregate.variable.value;
      await group.aggregators[variable].putBindings(bindings);
    }
  }
}
