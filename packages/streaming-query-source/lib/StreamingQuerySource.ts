import { EventEmitter } from 'events';
import type {
  BindingsStream,
  FragmentSelectorShape,
  IActionContext,
  IQuerySource,
  IQueryBindingsOptions,
  QuerySourceReference,
} from '@comunica/types';
import type { Quad } from '@rdfjs/types';
import type { AsyncIterator } from 'asynciterator';
import type { Ask, Operation, Update } from 'sparqlalgebrajs/lib/algebra';

export enum StreamingQuerySourceStatus {
  Initializing,
  Running,
  Idle,
  Stopped,
}

export abstract class StreamingQuerySource implements IQuerySource {
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

  public abstract halt(): void;

  public abstract resume(): void;

  public abstract getSelectorShape(_context: IActionContext): Promise<FragmentSelectorShape>;

  public abstract queryBindings(
    _operation: Operation,
    _context: IActionContext,
    _options: IQueryBindingsOptions | undefined,
  ): BindingsStream;

  public abstract queryBoolean(_operation: Ask, _context: IActionContext): Promise<boolean>;

  public abstract queryQuads(_operation: Operation, _context: IActionContext): AsyncIterator<Quad>;

  public abstract queryVoid(_operation: Update, _context: IActionContext): Promise<void>;

  public referenceValue: QuerySourceReference;
}
