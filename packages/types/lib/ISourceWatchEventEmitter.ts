import type { EventEmitter } from 'events';

export declare interface ISourceWatchEventEmitter extends EventEmitter {
  emit: ((event: 'update') => boolean) & ((event: 'delete') => boolean);
  on: ((event: 'update', listener: () => void) => this) & ((event: 'delete', listener: () => void) => this);
}
