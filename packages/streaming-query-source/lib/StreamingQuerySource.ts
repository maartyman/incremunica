import { EventEmitter } from 'events';
import type {
  BindingsStream,
  FragmentSelectorShape,
  IActionContext,
  IQuerySource,
  IQueryBindingsOptions,
  QuerySourceReference,
} from '@comunica/types';
import type { AsyncIterator } from 'asynciterator';
import type { Quad } from 'rdf-js';
import type { Ask, Operation, Update } from 'sparqlalgebrajs/lib/algebra';

export enum StreamingQuerySourceStatus {
  Initializing,
  Running,
  Idle,
  Stopped,
}

export class StreamingQuerySource implements IQuerySource {
  private _status: StreamingQuerySourceStatus = StreamingQuerySourceStatus.Initializing;
  public statusEvents: EventEmitter = new EventEmitter();
  public context?: IActionContext;

  public get status(): StreamingQuerySourceStatus {
    return this._status;
  }

  protected set status(value: StreamingQuerySourceStatus) {
    this._status = value;
    this.statusEvents.emit('status', value);
  }

  public async getSelectorShape(_context: IActionContext): Promise<FragmentSelectorShape> {
    throw new Error('Method not overridden in subclass');
  }

  public queryBindings(
    _operation: Operation,
    _context: IActionContext,
    _options: IQueryBindingsOptions | undefined,
  ): BindingsStream {
    throw new Error('Method not overridden in subclass');
  }

  public async queryBoolean(_operation: Ask, _context: IActionContext): Promise<boolean> {
    throw new Error('Method not overridden in subclass');
  }

  public queryQuads(_operation: Operation, _context: IActionContext): AsyncIterator<Quad> {
    throw new Error('Method not overridden in subclass');
  }

  public async queryVoid(_operation: Update, _context: IActionContext): Promise<void> {
    throw new Error('Method not overridden in subclass');
  }

  public referenceValue: QuerySourceReference;
}
