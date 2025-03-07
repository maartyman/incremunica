import { arrayifyStream } from 'arrayify-stream';
import { ArrayIterator } from 'asynciterator';
import { promisifyEventEmitter } from 'event-emitter-promisify/dist';
import { StartClosableMappingIterator } from '../lib/StartClosableMappingIterator';

describe('StartClosableIterator', () => {
  let startClosableIterator: StartClosableMappingIterator<any>;
  let onStart: jest.Mock;
  let onClose: jest.Mock;

  beforeEach(() => {
    onStart = jest.fn();
    onClose = jest.fn();
    startClosableIterator = new StartClosableMappingIterator(new ArrayIterator([ 1, 2 ]), undefined, {
      onStart,
      onClose,
    });
  });

  it('should work without options', async() => {
    startClosableIterator = new StartClosableMappingIterator(new ArrayIterator([ 1, 2 ]), undefined);
    const array = await arrayifyStream(startClosableIterator);
    expect(array).toEqual([ 1, 2 ]);
  });

  it('should work with a mapping function options', async() => {
    const map = jest.fn(item => item * 2);
    startClosableIterator = new StartClosableMappingIterator(new ArrayIterator([ 1, 2 ]), map);
    const array = await arrayifyStream(startClosableIterator);
    expect(array).toEqual([ 2, 4 ]);
  });

  it('should call onStart when read is called', () => {
    expect(onStart).not.toHaveBeenCalled();
    startClosableIterator.read();
    expect(onStart).toHaveBeenCalledWith();
  });

  it('should call onStart when on data is called', async() => {
    expect(onStart).not.toHaveBeenCalled();
    const array = await arrayifyStream(startClosableIterator);
    expect(onStart).toHaveBeenCalledWith();
    expect(array).toEqual([ 1, 2 ]);
  });

  it('should call onClose when it is closed', async() => {
    expect(onClose).not.toHaveBeenCalled();
    startClosableIterator.on('data', () => {});
    await promisifyEventEmitter(startClosableIterator);
    expect(onClose).toHaveBeenCalledWith();
  });

  it('should call onClose when it is destroyed without error message', () => {
    expect(onClose).not.toHaveBeenCalled();
    startClosableIterator.destroy();
    expect(onClose).toHaveBeenCalledWith();
  });
});
