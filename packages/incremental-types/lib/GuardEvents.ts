import type { EventEmitter } from 'events';

export interface IGuardEvents extends EventEmitter {
  emit: ((event: 'modified') => boolean) & ((event: 'up-to-date') => boolean);
  on: ((event: 'modified', listener: () => void) => this) & ((event: 'up-to-date', listener: () => void) => this);
}
