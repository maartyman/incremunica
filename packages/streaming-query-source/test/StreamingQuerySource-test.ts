import { EventEmitter } from 'events';
import type {
  IActionContext,
} from '@comunica/types';
import type { Operation, Ask, Update } from 'sparqlalgebrajs/lib/algebra';
import { StreamingQuerySource, StreamingQuerySourceStatus } from '../lib';

describe('StreamingQuerySource', () => {
  let source: StreamingQuerySource;

  beforeEach(() => {
    source = new StreamingQuerySource();
  });

  describe('constructor', () => {
    it('should initialize with a status of Initializing', () => {
      expect(source.status).toBe(StreamingQuerySourceStatus.Initializing);
    });

    it('should initialize statusEvents as an instance of EventEmitter', () => {
      expect(source.statusEvents).toBeInstanceOf(EventEmitter);
    });
  });

  describe('status', () => {
    it('should emit a status event when the status changes', () => {
      const statusSpy = jest.fn();
      source.statusEvents.on('status', statusSpy);

      (<any>source).status = StreamingQuerySourceStatus.Running;
      expect(statusSpy).toHaveBeenCalledWith(StreamingQuerySourceStatus.Running);

      (<any>source).status = StreamingQuerySourceStatus.Stopped;
      expect(statusSpy).toHaveBeenCalledWith(StreamingQuerySourceStatus.Stopped);
    });

    it('should update the internal _status when set', () => {
      (<any>source).status = StreamingQuerySourceStatus.Running;
      expect(source.status).toBe(StreamingQuerySourceStatus.Running);
    });
  });

  describe('getSelectorShape', () => {
    it('should throw an error when not overridden', async() => {
      await expect(source.getSelectorShape(<IActionContext>{}))
        .rejects.toThrow('Method not overridden in subclass');
    });
  });

  describe('queryBindings', () => {
    it('should throw an error when not overridden', () => {
      expect(() => source.queryBindings(<Operation>{}, <IActionContext>{}, undefined)).toThrow(
        'Method not overridden in subclass',
      );
    });
  });

  describe('queryBoolean', () => {
    it('should throw an error when not overridden', async() => {
      await expect(source.queryBoolean(<Ask>{}, <IActionContext>{}))
        .rejects.toThrow('Method not overridden in subclass');
    });
  });

  describe('queryQuads', () => {
    it('should throw an error when not overridden', () => {
      expect(() => source.queryQuads(<Operation>{}, <IActionContext>{})).toThrow(
        'Method not overridden in subclass',
      );
    });
  });

  describe('queryVoid', () => {
    it('should throw an error when not overridden', async() => {
      await expect(source.queryVoid(<Update>{}, <IActionContext>{}))
        .rejects.toThrow('Method not overridden in subclass');
    });
  });
});
