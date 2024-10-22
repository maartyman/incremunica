import { BindingsToQuadsIterator } from '@comunica/actor-query-operation-construct';
import { QuerySourceRdfJs } from '@comunica/actor-query-source-identify-rdfjs';
import type { MediatorQueryOperation } from '@comunica/bus-query-operation';
import { KeysQueryOperation } from '@comunica/context-entries';
import { ActionContext } from '@comunica/core';
import type { ComunicaDataFactory, IActionContext, IJoinEntry } from '@comunica/types';
import type { Bindings, BindingsFactory } from '@comunica/utils-bindings-factory';
import { getSafeBindings, materializeOperation } from '@comunica/utils-query-operation';
import { ActionContextKeyIsAddition } from '@incremunica/actor-merge-bindings-context-is-addition';
import { KeysDeltaQueryJoin } from '@incremunica/context-entries';
import type { Quad } from '@incremunica/incremental-types';
import { AsyncIterator } from 'asynciterator';
import { Store } from 'n3';
import type { Factory } from 'sparqlalgebrajs';

export class DeltaQueryIterator extends AsyncIterator<Bindings> {
  private count = 0;
  private currentSource?: AsyncIterator<Bindings> = undefined;
  protected destroySources = true;
  public readonly algebraFactory: Factory;
  private readonly entries: IJoinEntry[];
  private readonly mediatorQueryOperation: MediatorQueryOperation;
  private readonly subContext: IActionContext;
  private readonly store = new Store();
  private pending = false;
  private readonly bindingsFactory: BindingsFactory;

  public constructor(
    entries: IJoinEntry[],
    mediatorQueryOperation: MediatorQueryOperation,
    dataFactory: ComunicaDataFactory,
    algebraFactory: Factory,
    bindingsFactory: BindingsFactory,
  ) {
    super();
    this.algebraFactory = algebraFactory;
    this.bindingsFactory = bindingsFactory;
    this.entries = entries;
    this.subContext = new ActionContext()
      .set(KeysQueryOperation.querySources, [{
        source: new QuerySourceRdfJs(this.store, dataFactory, this.bindingsFactory),
      }])
      .set(KeysDeltaQueryJoin.fromDeltaQuery, true);
    this.mediatorQueryOperation = mediatorQueryOperation;

    this.readable = false;

    for (const entry of this.entries) {
      if (entry.output.bindingsStream.readable) {
        this.readable = true;
      }

      entry.output.bindingsStream.on('end', () => {
        this.readable = true;
      });

      entry.output.bindingsStream.on('error', error => this.destroy(error));

      entry.output.bindingsStream.on('readable', () => {
        this.readable = true;
      });
    }
  }

  public override read(): Bindings | null {
    if (this.currentSource !== undefined && !this.currentSource.done && this.currentSource.readable) {
      const binding = this.currentSource.read();
      if (binding !== null) {
        return binding;
      }
    }

    this.readable = false;

    // Close this iterator if all of its sources have been read
    for (const entry of this.entries) {
      if (!entry.output.bindingsStream.done) {
        this.getNewBindingsStream();
        return null;
      }
    }
    this.close();
    return null;
  }

  private getNewBindingsStream(): void {
    if (
      this.readable ||
      !(this.currentSource === undefined || this.currentSource.done) ||
      this.pending ||
      this.closed
    ) {
      return;
    }
    for (const _nonUsedVar of this.entries) {
      this.count++;
      this.count %= this.entries.length;

      const source = this.entries[this.count];

      if (!source.output.bindingsStream.readable) {
        continue;
      }

      let bindings = (<AsyncIterator<Bindings>><unknown>source.output.bindingsStream).read();
      if (bindings === null) {
        continue;
      }
      // TODO check if this casting is correct
      let quad = BindingsToQuadsIterator.bindQuad(bindings, <Quad><any>source.operation);

      while (quad === undefined) {
        bindings = (<AsyncIterator<Bindings>><unknown>source.output.bindingsStream).read();
        if (bindings === null) {
          break;
        }
        quad = BindingsToQuadsIterator.bindQuad(bindings, <Quad><any>source.operation);
      }
      if (bindings === null || quad === undefined) {
        continue;
      }

      const constBindings = bindings;
      const constQuad = quad;
      const subEntries = [ ...this.entries ];
      subEntries[this.count] = subEntries.at(-1)!;
      subEntries.pop();

      // Do query
      this.pending = true;
      const subOperations = subEntries
        .map(entry =>
          materializeOperation(
            entry.operation,
            constBindings,
            this.algebraFactory,
            this.bindingsFactory,
            { bindFilter: false },
          ));

      const operation = subOperations.length === 1 ?
        subOperations[0] :
        this.algebraFactory.createJoin(subOperations);

      this.mediatorQueryOperation.mediate(
        { operation, context: this.subContext },
      ).then((unsafeOutput) => {
        const output = getSafeBindings(unsafeOutput);
        // Figure out diff (change diff if needed)
        let bindingsStream: AsyncIterator<Bindings> | undefined =
          (<AsyncIterator<Bindings>><unknown>output.bindingsStream).map(
            (resultBindings) => {
              const tempBindings = resultBindings.merge(constBindings);
              if (tempBindings === undefined) {
                return null;
              }
              // TODO I don't think this is needed
              // tempBindings = tempBindings.setContextEntry(
              // new ActionContextKeyIsAddition(), constBindings.getContextEntry(new ActionContextKeyIsAddition())
              // );
              return tempBindings;
            },
          );

        this.pending = false;

        bindingsStream.once('end', () => {
          // Add or delete binding from store
          if (constBindings.getContextEntry(new ActionContextKeyIsAddition()) ?? true) {
            this.store.add(constQuad);
          } else {
            this.store.delete(constQuad);
          }
          this.currentSource = undefined;
          // TODO why set it to undefined?
          bindingsStream = undefined;
          this.readable = true;
        });

        bindingsStream.on('readable', () => {
          this.readable = true;
        });

        this.currentSource = bindingsStream;
      }).catch(() => {
        this.pending = false;
        this.currentSource = undefined;
        this.readable = true;
      });
      return;
    }
  }

  protected override _end(destroy = false): void {
    super._end(destroy);

    // Destroy all sources that are still readable
    if (this.destroySources) {
      for (const source of this.entries) {
        source.output.bindingsStream.destroy();
      }
    }
  }
}
