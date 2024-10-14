import { BindingsToQuadsIterator } from '@comunica/actor-query-operation-construct';
import { ActorQueryOperation, materializeOperation } from '@comunica/bus-query-operation';
import type { MediatorQueryOperation } from '@comunica/bus-query-operation';
import { ActionContext } from '@comunica/core';
import type { IActionContext, IJoinEntry } from '@comunica/types';
import { KeysDeltaQueryJoin } from '@incremunica/context-entries';
import type { Quad } from '@incremunica/incremental-types';
import type { BindingsStream } from '@comunica/types';
import {Bindings, BindingsFactory} from '@comunica/bindings-factory';
import { AsyncIterator } from 'asynciterator';
import { Store } from 'n3';
import { Factory } from 'sparqlalgebrajs';
import {KeysQueryOperation} from "@comunica/context-entries";
import {ActionContextKeyIsAddition} from "@incremunica/actor-merge-bindings-context-is-addition";

export class DeltaQueryIterator extends AsyncIterator<Bindings> {
  private count = 0;
  private currentSource?: BindingsStream = undefined;
  protected destroySources = true;
  public static readonly FACTORY = new Factory();
  private readonly entries: IJoinEntry[];
  private readonly mediatorQueryOperation: MediatorQueryOperation;
  private readonly subContext: IActionContext;
  private readonly store = new Store();
  private pending = false;
  private readonly bindingsFactory = new BindingsFactory();

  public constructor(
    entries: IJoinEntry[],
    context: IActionContext,
    mediatorQueryOperation: MediatorQueryOperation,
  ) {
    super();
    this.entries = entries;
    this.subContext = new ActionContext()
      .set(KeysQueryOperation.querySources, [ this.store ])
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

  public read(): Bindings | null {
    if (this.currentSource !== undefined && !this.currentSource.done && this.currentSource.readable) {
      const binding = <Bindings>this.currentSource.read();
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
    for (const nonUsedVar of this.entries) {
      this.count++;
      this.count %= this.entries.length;

      const source = this.entries[this.count];

      if (!source.output.bindingsStream.readable) {
        continue;
      }

      let bindings = source.output.bindingsStream.read();
      if (bindings === null) {
        continue;
      }
      let quad = BindingsToQuadsIterator.bindQuad(bindings, <Quad><any>source.operation);

      while (quad === undefined) {
        bindings = source.output.bindingsStream.read();
        if (bindings === null) {
          break;
        }
        quad = BindingsToQuadsIterator.bindQuad(bindings, <Quad><any>source.operation);
      }
      if (bindings === null || quad === undefined) {
        continue;
      }

      const constBindings = <Bindings>bindings;
      const constQuad = quad;
      const subEntries = [ ...this.entries ];
      subEntries[this.count] = subEntries[subEntries.length - 1];
      subEntries.pop();

      // Do query
      this.pending = true;
      const subOperations = subEntries
        .map(entry => materializeOperation(entry.operation, constBindings, this.bindingsFactory, { bindFilter: false }));

      const operation = subOperations.length === 1 ?
        subOperations[0] :
        DeltaQueryIterator.FACTORY.createJoin(subOperations);

      this.mediatorQueryOperation.mediate(
        { operation, context: this.subContext },
      ).then(unsafeOutput => {
        const output = ActorQueryOperation.getSafeBindings(unsafeOutput);
        // Figure out diff (change diff if needed)
        let bindingsStream: BindingsStream | undefined = <BindingsStream><unknown>output.bindingsStream.map(
          (resultBindings: Bindings) => {
            let tempBindings = <Bindings>resultBindings.merge(constBindings);
            if (tempBindings === undefined) {
              return null;
            }
            tempBindings = tempBindings.setContextEntry(new ActionContextKeyIsAddition(), constBindings.getContextEntry(new ActionContextKeyIsAddition()));
            return tempBindings;
          },
        );

        this.pending = false;

        bindingsStream.once('end', () => {
          // Add or delete binding from store
          if (constBindings.getContextEntry(new ActionContextKeyIsAddition()) ||
            constBindings.getContextEntry(new ActionContextKeyIsAddition()) === undefined) {
            this.store.add(constQuad);
          } else {
            this.store.delete(constQuad);
          }
          this.currentSource = undefined;
          bindingsStream = undefined;
          this.readable = true;
        });

        bindingsStream.on('readable', () => {
          this.readable = true;
        });

        this.currentSource = <BindingsStream><unknown>bindingsStream;
      }).catch(() => {
        this.pending = false;
        this.currentSource = undefined;
        this.readable = true;
      });
      return;
    }
  }

  protected _end(destroy = false): void {
    super._end(destroy);

    // Destroy all sources that are still readable
    if (this.destroySources) {
      for (const source of this.entries) {
        source.output.bindingsStream.destroy();
      }
    }
  }
}

