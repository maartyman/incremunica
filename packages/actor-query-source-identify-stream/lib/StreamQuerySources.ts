import { EventEmitter } from 'events';
import {
  LinkedRdfSourcesAsyncRdfIterator,
} from '@comunica/actor-query-source-identify-hypermedia/lib/LinkedRdfSourcesAsyncRdfIterator';
import type { IActorQuerySourceIdentifyOutput, MediatorQuerySourceIdentify } from '@comunica/bus-query-source-identify';
import type {
  Bindings,
  BindingsStream,
  ComunicaDataFactory,
  FragmentSelectorShape,
  IActionContext,
  IQueryBindingsOptions,
  IQuerySource,
} from '@comunica/types';
import { MetadataValidationState } from '@comunica/utils-metadata';
import type * as RDF from '@rdfjs/types';
import { AsyncIterator, UnionIterator } from 'asynciterator';
import { Queue } from 'data-structure-typed';
import { type Algebra, Factory } from 'sparqlalgebrajs';
import type { Operation } from 'sparqlalgebrajs/lib/algebra';

export type IStreamQuerySource = {
  isAddition: boolean;
  value: string;
};

type ISourceWrapper = {
  source: IQuerySource | undefined;
  deleteCallbacks: (() => void)[];
};

type ISourceWrapperSafe = {
  source: IQuerySource;
  deleteCallbacks: (() => void)[];
};

export class StreamQuerySources implements IQuerySource {
  public referenceValue: string;
  public context?: IActionContext;
  private readonly sources: Map<string, ISourceWrapper> = new Map();
  private readonly sourcesEventEmitter: EventEmitter;
  protected readonly selectorShape: FragmentSelectorShape;
  private readonly dataFactory: ComunicaDataFactory;

  public constructor(
    stream: AsyncIterator<IStreamQuerySource>,
    dataFactory: ComunicaDataFactory,
    mediatorQuerySourceIdentify: MediatorQuerySourceIdentify,
    context: IActionContext,
  ) {
    this.sourcesEventEmitter = new EventEmitter();
    this.sourcesEventEmitter.setMaxListeners(Number.POSITIVE_INFINITY);
    stream.on('data', (item: IStreamQuerySource) => {
      if (item.isAddition) {
        const chunk: ISourceWrapper = {
          deleteCallbacks: [],
          source: undefined,
        };
        mediatorQuerySourceIdentify
          .mediate({ context, querySourceUnidentified: { value: item.value }})
          .then((querySource: IActorQuerySourceIdentifyOutput) => {
            chunk.source = querySource.querySource.source;
            this.sourcesEventEmitter.emit('data', chunk);
          })
          .catch((error: Error) => {
            throw error;
          });
        this.sources.set(item.value, chunk);
      } else {
        const source = this.sources.get(item.value);
        if (!source) {
          return;
        }
        for (const deleteCallback of source.deleteCallbacks) {
          deleteCallback();
        }
        this.sources.delete(item.value);
      }
    });
    this.referenceValue = 'StreamingHypermediaQuerySources';
    this.dataFactory = dataFactory;
    const AF = new Factory(<RDF.DataFactory> this.dataFactory);
    this.selectorShape = {
      type: 'operation',
      operation: {
        operationType: 'pattern',
        pattern: AF.createPattern(
          this.dataFactory.variable('s'),
          this.dataFactory.variable('p'),
          this.dataFactory.variable('o'),
        ),
      },
      variablesOptional: [
        this.dataFactory.variable('s'),
        this.dataFactory.variable('p'),
        this.dataFactory.variable('o'),
      ],
    };
  }

  public async getSelectorShape(): Promise<FragmentSelectorShape> {
    return this.selectorShape;
  }

  public queryBindings(
    operation: Operation,
    context: IActionContext,
    options: IQueryBindingsOptions | undefined,
  ): BindingsStream {
    const buffer = new Queue<ISourceWrapperSafe>();
    for (const sourceWrapper of this.sources.values()) {
      if (sourceWrapper.source === undefined) {
        continue;
      }
      buffer.push(<ISourceWrapperSafe>sourceWrapper);
    }

    const iterator = new AsyncIterator<BindingsStream>();
    iterator.read = (): BindingsStream | null => {
      const sourceWrapper = buffer.shift();
      if (sourceWrapper === undefined) {
        iterator.readable = false;
        return null;
      }
      const bindingsStream = sourceWrapper.source.queryBindings(operation, context, options);
      let stopStreamFn = bindingsStream.getProperty<() => void>('delete');
      if (!stopStreamFn) {
        if (bindingsStream instanceof LinkedRdfSourcesAsyncRdfIterator) {
          stopStreamFn = () => {
            for (const currentIterator of (<any>bindingsStream).currentIterators) {
              const fn = (<BindingsStream>currentIterator).getProperty<() => void>('delete');
              if (fn) {
                fn();
              } else {
                throw new Error('No delete function found');
              }
            }
          };
        } else {
          throw new TypeError('No delete function found');
        }
      }
      sourceWrapper.deleteCallbacks.push(stopStreamFn);
      return bindingsStream;
    };
    iterator.readable = true;

    const addSourceToBuffer = (sourceWrapper: ISourceWrapperSafe): void => {
      if (iterator.done) {
        this.sourcesEventEmitter.removeListener('data', addSourceToBuffer);
        return;
      }
      buffer.push(sourceWrapper);
      iterator.readable = true;
    };
    this.sourcesEventEmitter.on('data', addSourceToBuffer);

    const unionIterator = new UnionIterator<Bindings>(iterator, { autoStart: false });

    unionIterator.setProperty('metadata', {
      state: new MetadataValidationState(),
      cardinality: { type: 'exact', value: 1 },
      variables: [
        {
          variable: this.dataFactory.variable('s'),
          canBeUndef: false,
        },
        {
          variable: this.dataFactory.variable('p'),
          canBeUndef: false,
        },
        {
          variable: this.dataFactory.variable('o'),
          canBeUndef: false,
        },
      ],
    });

    return unionIterator;
  }

  public queryQuads(
    _operation: Algebra.Operation,
    _context: IActionContext,
  ): AsyncIterator<RDF.Quad> {
    throw new Error('queryQuads is not implemented in StreamingQuerySourceRdfJs');
  }

  public queryBoolean(
    _operation: Algebra.Ask,
    _context: IActionContext,
  ): Promise<boolean> {
    throw new Error('queryBoolean is not implemented in StreamingQuerySourceRdfJs');
  }

  public queryVoid(
    _operation: Algebra.Update,
    _context: IActionContext,
  ): Promise<void> {
    throw new Error('queryVoid is not implemented in StreamingQuerySourceRdfJs');
  }

  public toString(): string {
    return `StreamingHypermediaQuerySources`;
  }
}
