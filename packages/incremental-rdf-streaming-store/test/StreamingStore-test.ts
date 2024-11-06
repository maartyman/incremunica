import 'jest-rdf';
import type { Quad } from '@incremunica/incremental-types';
import arrayifyStream from 'arrayify-stream';
import { promisifyEventEmitter } from 'event-emitter-promisify/dist';
import { Store } from 'n3';
import { DataFactory } from 'rdf-data-factory';
import { Readable } from 'readable-stream';
import { StreamingStore } from '../lib/StreamingStore';

const quad = require('rdf-quad');
const streamifyArray = require('streamify-array');

const DF = new DataFactory();

describe('StreamStore', () => {
  let store: StreamingStore<Quad>;

  beforeEach(() => {
    store = new StreamingStore();
  });

  it('exposes the internal store', async() => {
    expect(store.getStore()).toBeInstanceOf(Store);
  });

  it('handles an empty ended store', async() => {
    store.end();
    await expect(arrayifyStream(store.match())).resolves
      .toEqual([]);
  });

  it('handles an empty non-ended store', async() => {
    const stream = store.match();
    jest.spyOn(stream, 'on');
    await new Promise(setImmediate);
    expect(stream.on).not.toHaveBeenCalled();
  });

  it('throws when importing to an ended store', async() => {
    store.end();
    expect(() => store.import(<any>undefined))
      .toThrow('Attempted to import into an ended StreamingStore');
  });

  it('copy of store is of type Store', async() => {
    expect(store.copyOfStore()).toBeInstanceOf(Store);
  });

  it('copy of store has all quads', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s', 'p', 'o'),
    ])));

    await expect(
      arrayifyStream(store.copyOfStore().match()),
    ).resolves.toBeRdfIsomorphic([
      quad('s', 'p', 'o'),
    ]);
  });

  it('should return hasEnded', async() => {
    expect(store.hasEnded()).toBeFalsy();
    store.end();
    expect(store.hasEnded()).toBeTruthy();
  });

  it('should end if match stream is destroyed', async() => {
    const stream = <Readable>store.match();
    expect(store.hasEnded()).toBeFalsy();
    stream.destroy();
    await new Promise<void>(resolve => stream.on('close', () => resolve()));
    expect(store.hasEnded()).toBeTruthy();
  });

  it('should only end if all match streams are destroyed', async() => {
    const stream1 = <Readable>store.match();
    const stream2 = <Readable>store.match();
    const stream3 = <Readable>store.match();
    expect(store.hasEnded()).toBeFalsy();
    stream1.destroy();
    await new Promise<void>(resolve => stream1.on('close', () => resolve()));
    expect(store.hasEnded()).toBeFalsy();
    stream2.destroy();
    await new Promise<void>(resolve => stream2.on('close', () => resolve()));
    expect(store.hasEnded()).toBeFalsy();
    stream3.destroy();
    await new Promise<void>(resolve => stream3.on('close', () => resolve()));
    expect(store.hasEnded()).toBeTruthy();
  });

  it('handle deletion of quads', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s3', 'p3', 'o3'),
    ])));

    const quad1 = <Quad>quad('s1', 'p1', 'o1');
    const quad2 = <Quad>quad('s2', 'p2', 'o2');
    quad1.isAddition = false;
    quad2.isAddition = false;

    await promisifyEventEmitter(store.import(streamifyArray([
      quad1,
      quad2,
    ])));

    await expect(
      arrayifyStream(store.copyOfStore().match()),
    ).resolves.toBeRdfIsomorphic([
      quad('s3', 'p3', 'o3'),
    ]);
  });

  it('gracefully handles ending during slow imports', async() => {
    const readStream = store.match();

    const importStream = new Readable({ objectMode: true });
    importStream._read = () => {
      setImmediate(() => {
        importStream.push(quad('s1', 'p1', 'o1'));
        importStream.push(null);
      });
    };
    store.import(importStream);
    store.end();
    await new Promise(resolve => importStream.on('end', resolve));

    readStream.on('data', () => {
      // Void reads
    });
    const errorHandler = jest.fn();
    readStream.on('error', errorHandler);
    await new Promise(resolve => readStream.on('end', resolve));

    expect(errorHandler).not.toHaveBeenCalled();
  });

  it('gracefully handles ending during very slow imports', async() => {
    const readStream = store.match();

    const importStream = new Readable({ objectMode: true });
    importStream._read = () => {
      // Do nothing
    };
    store.import(importStream);
    store.end();

    readStream.on('data', () => {
      // Void reads
    });
    const errorHandler = jest.fn();
    readStream.on('error', errorHandler);
    await new Promise(resolve => readStream.on('end', resolve));

    importStream.push(quad('s1', 'p1', 'o1'));
    importStream.push(null);

    expect(errorHandler).not.toHaveBeenCalled();
  });

  it('handles one match after end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ])));
    store.end();

    await expect(arrayifyStream(store.match())).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ]);
  });

  it('handles one match with multiple imports after end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
    ])));
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s2', 'p2', 'o2'),
    ])));
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));
    store.end();

    await expect(arrayifyStream(store.match())).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
  });

  it('handles one match before end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ])));
    const match1 = store.match();
    store.end();

    await expect(arrayifyStream(match1)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ]);
  });

  it('handles one match with multiple imports before end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
    ])));
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s2', 'p2', 'o2'),
    ])));
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));
    const match1 = store.match();
    store.end();

    await expect(arrayifyStream(match1)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
  });

  it('handles multiple matches after end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ])));
    store.end();
    const match1 = store.match();
    const match2 = store.match();
    const match3 = store.match();

    await expect(arrayifyStream(match1)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ]);
    await expect(arrayifyStream(match2)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ]);
    await expect(arrayifyStream(match3)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ]);
  });

  it('handles multiple matches with multiple imports after end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
    ])));
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s2', 'p2', 'o2'),
    ])));
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));
    store.end();
    const match1 = store.match();
    const match2 = store.match();
    const match3 = store.match();

    await expect(arrayifyStream(match1)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
    await expect(arrayifyStream(match2)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
    await expect(arrayifyStream(match3)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
  });

  it('handles multiple matches before end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ])));
    const match1 = store.match();
    const match2 = store.match();
    const match3 = store.match();
    store.end();

    await expect(arrayifyStream(match1)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ]);
    await expect(arrayifyStream(match2)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ]);
    await expect(arrayifyStream(match3)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ]);
  });

  it('handles multiple matches with multiple imports before end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
    ])));
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s2', 'p2', 'o2'),
    ])));
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));
    const match1 = store.match();
    const match2 = store.match();
    const match3 = store.match();
    store.end();

    await expect(arrayifyStream(match1)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
    await expect(arrayifyStream(match2)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
    await expect(arrayifyStream(match3)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
  });

  it('handles one match with imports before and after end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
    ])));

    const match1 = store.match();

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s2', 'p2', 'o2'),
    ])));

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s3', 'p3', 'o3'),
    ])));

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s4', 'p4', 'o4'),
    ])));

    store.end();

    await expect(arrayifyStream(match1)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
  });

  it('handles multiple matches with imports before and after end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
    ])));

    const match1 = store.match();
    const match2 = store.match();

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s2', 'p2', 'o2'),
    ])));

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));

    store.end();

    await expect(arrayifyStream(match1)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);

    await expect(arrayifyStream(match2)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
  });

  it('handles multiple async matches with imports before and after end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
    ])));
    const match1 = store.match();

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s2', 'p2', 'o2'),
    ])));

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));

    store.end();

    await expect(arrayifyStream(store.match())).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);

    await expect(arrayifyStream(match1)).resolves
      .toBeRdfIsomorphic([
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
  });

  it('handles matches for different quad patterns', async() => {
    const m1 = store.match();
    const ms1 = store.match(DF.namedNode('s'));
    const ms2 = store.match(DF.namedNode('s'), DF.namedNode('p'));
    const ms3 = store.match(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('o'));
    const ms4 = store
      .match(DF.namedNode('s'), DF.namedNode('p'), DF.namedNode('o'), DF.namedNode('g'));
    const mp1 = store.match(undefined, DF.namedNode('p'));
    const mp2 = store.match(undefined, DF.namedNode('p'), DF.namedNode('o'));
    const mp3 = store.match(undefined, DF.namedNode('p'), DF.namedNode('o'), DF.namedNode('g'));
    const mo1 = store.match(undefined, undefined, DF.namedNode('o'));
    const mo2 = store.match(undefined, undefined, DF.namedNode('o'), DF.namedNode('g'));
    const mg1 = store.match(undefined, undefined, undefined, DF.namedNode('g'));

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s', 'p1', 'o1'),
      quad('s', 'p', 'o2'),
      quad('s3', 'p', 'o'),
      quad('s4', 'p4', 'o', 'g'),
    ])));

    store.end();

    await expect(arrayifyStream(m1)).resolves
      .toBeRdfIsomorphic([
        quad('s', 'p1', 'o1'),
        quad('s', 'p', 'o2'),
        quad('s3', 'p', 'o'),
        quad('s4', 'p4', 'o', 'g'),
      ]);

    await expect(arrayifyStream(ms1)).resolves
      .toBeRdfIsomorphic([
        quad('s', 'p1', 'o1'),
        quad('s', 'p', 'o2'),
      ]);
    await expect(arrayifyStream(ms2)).resolves
      .toBeRdfIsomorphic([
        quad('s', 'p', 'o2'),
      ]);
    await expect(arrayifyStream(ms3)).resolves
      .toBeRdfIsomorphic([]);
    await expect(arrayifyStream(ms4)).resolves
      .toBeRdfIsomorphic([]);

    await expect(arrayifyStream(mp1)).resolves
      .toBeRdfIsomorphic([
        quad('s', 'p', 'o2'),
        quad('s3', 'p', 'o'),
      ]);
    await expect(arrayifyStream(mp2)).resolves
      .toBeRdfIsomorphic([
        quad('s3', 'p', 'o'),
      ]);
    await expect(arrayifyStream(mp3)).resolves
      .toBeRdfIsomorphic([]);

    await expect(arrayifyStream(mo1)).resolves
      .toBeRdfIsomorphic([
        quad('s3', 'p', 'o'),
        quad('s4', 'p4', 'o', 'g'),
      ]);
    await expect(arrayifyStream(mo2)).resolves
      .toBeRdfIsomorphic([
        quad('s4', 'p4', 'o', 'g'),
      ]);

    await expect(arrayifyStream(mg1)).resolves
      .toBeRdfIsomorphic([
        quad('s4', 'p4', 'o', 'g'),
      ]);
  });

  it('handles multiple matches with removes before and after end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));
    await promisifyEventEmitter(store.remove(streamifyArray([
      quad('s1', 'p1', 'o1'),
    ])));
    const match1 = store.match();
    const match2 = store.match();
    const posQuad = quad('s5', 'p5', 'o5');
    posQuad.isAddition = true;

    await promisifyEventEmitter(store.remove(streamifyArray([
      quad('s2', 'p2', 'o2'),
    ])));

    await promisifyEventEmitter(store.remove(streamifyArray([
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
      posQuad,
    ])));

    store.end();

    await expect(arrayifyStream(match1)).resolves
      .toBeRdfIsomorphic([
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
        quad('s5', 'p5', 'o5'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
    await expect(arrayifyStream(match2)).resolves
      .toBeRdfIsomorphic([
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
        quad('s5', 'p5', 'o5'),
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
  });

  it('handles multiple async matches with removes before and after end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));
    await promisifyEventEmitter(store.remove(streamifyArray([
      quad('s1', 'p1', 'o1'),
    ])));
    const match1 = store.match();

    await promisifyEventEmitter(store.remove(streamifyArray([
      quad('s2', 'p2', 'o2'),
    ])));

    await promisifyEventEmitter(store.remove(streamifyArray([
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));
    store.end();

    await expect(arrayifyStream(store.match())).resolves
      .toBeRdfIsomorphic([]);

    await expect(arrayifyStream(match1)).resolves
      .toBeRdfIsomorphic([
        quad('s2', 'p2', 'o2'),
        quad('s3', 'p3', 'o3'),
        quad('s4', 'p4', 'o4'),
      ]);
  });

  it('trows error for remove after end', async() => {
    store.end();

    expect(() => {
      store.remove(streamifyArray([
        quad('s1', 'p1', 'o1'),
      ]));
    }).toThrow('Attempted to remove out of an ended StreamingStore');
  });

  it('handles halting', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ])));

    const matchStream = store.match();

    expect(store.isHalted()).toBeFalsy();
    store.halt();
    expect(store.isHalted()).toBeTruthy();

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));

    await promisifyEventEmitter(store.remove(streamifyArray([
      quad('s4', 'p4', 'o4'),
    ])));

    await expect(arrayifyStream(store.copyOfStore().match())).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ]);

    store.resume();

    await expect(arrayifyStream(store.copyOfStore().match())).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s3', 'p3', 'o3'),
    ]);

    store.end();

    await expect(arrayifyStream(matchStream)).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
      quad('s4', 'p4', 'o4'),
    ]);
  });

  it('handles end during halting', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ])));

    const matchStream = store.match();

    expect(store.isHalted()).toBeFalsy();
    store.halt();
    expect(store.isHalted()).toBeTruthy();

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));

    store.end();

    await expect(arrayifyStream(store.copyOfStore().match())).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ]);

    store.resume();

    await expect(arrayifyStream(store.copyOfStore().match())).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ]);

    await expect(arrayifyStream(matchStream)).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ]);
  });

  it('stopMatch should stop match', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ])));

    const options = { stopMatch: () => {} };

    const matchStream = store.match(
      null,
      null,
      null,
      null,
      options,
    );

    options.stopMatch();

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));

    await expect(arrayifyStream(matchStream)).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ]);

    store.end();

    await expect(arrayifyStream(store.copyOfStore().match())).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ]);
  });

  it('stopMatch should stop match with multiple match', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
    ])));

    const options1 = { stopMatch: () => {} };
    const matchStream1 = store.match(
      null,
      null,
      null,
      null,
      options1,
    );

    const options2 = { stopMatch: () => {} };
    const matchStream2 = store.match(
      null,
      null,
      null,
      null,
      options2,
    );

    options1.stopMatch();

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s2', 'p2', 'o2'),
    ])));

    options2.stopMatch();

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s3', 'p3', 'o3'),
    ])));

    await expect(arrayifyStream(matchStream1)).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
    ]);

    await expect(arrayifyStream(matchStream2)).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ]);

    store.end();

    await expect(arrayifyStream(store.copyOfStore().match())).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s3', 'p3', 'o3'),
    ]);
  });

  it('stopMatch after end', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ])));

    store.end();

    const options = { stopMatch: () => {} };

    const matchStream = store.match(
      null,
      null,
      null,
      null,
      options,
    );

    options.stopMatch(); // Does noting

    await expect(arrayifyStream(matchStream)).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ]);
  });

  it('handle stopMatch with halt', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ])));

    const options = { stopMatch: () => {} };

    const matchStream = store.match(
      null,
      null,
      null,
      null,
      options,
    );

    store.halt();
    options.stopMatch();

    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ])));

    await expect(arrayifyStream(matchStream)).resolves.toBeRdfIsomorphic(
      await arrayifyStream(store.copyOfStore().match()),
    );

    store.resume();
    store.end();

    await expect(arrayifyStream(store.copyOfStore().match())).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
    ]);
  });

  it('handle stopMatch with halt with deletions', async() => {
    await promisifyEventEmitter(store.import(streamifyArray([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ])));

    const options = { stopMatch: () => {} };

    const matchStream = store.match(
      null,
      null,
      null,
      null,
      options,
    );

    store.halt();
    options.stopMatch();

    const quad1 = quad('s1', 'p1', 'o1');
    const quad2 = quad('s2', 'p2', 'o2');

    quad1.isAddition = false;
    quad2.isAddition = false;

    await promisifyEventEmitter(store.import(streamifyArray([
      quad1,
      quad2,
    ])));

    await expect(arrayifyStream(matchStream)).resolves.toBeRdfIsomorphic(
      await arrayifyStream(store.copyOfStore().match()),
    );

    store.resume();
    store.end();

    await expect(arrayifyStream(store.copyOfStore().match())).resolves.toBeRdfIsomorphic([]);
  });

  it('should handle single quad insertions', async() => {
    store.addQuad(quad('s1', 'p1', 'o1'));

    const match = store.match();

    await expect(
      arrayifyStream(store.copyOfStore().match()),
    ).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
    ]);

    const quad1 = <Quad>quad('s2', 'p2', 'o2');
    quad1.isAddition = true;
    store.addQuad(quad1);

    await expect(
      arrayifyStream(store.copyOfStore().match()),
    ).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ]);

    const quad2 = <Quad>quad('s2', 'p2', 'o2');
    quad2.isAddition = false;
    store.addQuad(quad2);

    await expect(
      arrayifyStream(store.copyOfStore().match()),
    ).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
    ]);

    store.end();

    await expect(arrayifyStream(match)).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s2', 'p2', 'o2'),
    ]);
  });

  it('should handle single quad removals', async() => {
    const quad1 = <Quad>quad('s1', 'p1', 'o1');
    quad1.isAddition = true;
    store.removeQuad(quad1);

    const quad2 = <Quad>quad('s2', 'p2', 'o2');
    quad2.isAddition = true;
    store.removeQuad(quad2);

    const match = store.match();

    await expect(
      arrayifyStream(store.copyOfStore().match()),
    ).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ]);

    store.removeQuad(quad('s1', 'p1', 'o1'));

    await expect(
      arrayifyStream(store.copyOfStore().match()),
    ).resolves.toBeRdfIsomorphic([
      quad('s2', 'p2', 'o2'),
    ]);

    const quad3 = <Quad>quad('s2', 'p2', 'o2');
    quad3.isAddition = false;
    store.addQuad(quad3);

    await expect(
      arrayifyStream(store.copyOfStore().match()),
    ).resolves.toBeRdfIsomorphic([
    ]);

    store.end();

    await expect(arrayifyStream(match)).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ]);
  });

  it('should throw error if addQuad after end', async() => {
    store.end();

    expect(() => {
      store.addQuad(quad('s1', 'p1', 'o1'));
    }).toThrow('Attempted to add a quad into an ended StreamingStore.');
  });

  it('should throw error if removeQuad after end', async() => {
    store.end();

    expect(() => {
      store.removeQuad(quad('s1', 'p1', 'o1'));
    }).toThrow('Attempted to remove a quad of an ended StreamingStore.');
  });

  it('handles halting with addQuad', async() => {
    store.addQuad(quad('s1', 'p1', 'o1'));
    store.addQuad(quad('s2', 'p2', 'o2'));

    const matchStream = store.match();

    expect(store.isHalted()).toBeFalsy();
    store.halt();
    expect(store.isHalted()).toBeTruthy();

    store.addQuad(quad('s3', 'p3', 'o3'));
    store.addQuad(quad('s4', 'p4', 'o4'));

    store.removeQuad(quad('s4', 'p4', 'o4'));

    await expect(arrayifyStream(store.copyOfStore().match())).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ]);

    store.resume();

    await expect(arrayifyStream(store.copyOfStore().match())).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s3', 'p3', 'o3'),
    ]);

    store.end();

    await expect(arrayifyStream(matchStream)).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
      quad('s3', 'p3', 'o3'),
      quad('s4', 'p4', 'o4'),
      quad('s4', 'p4', 'o4'),
    ]);
  });

  it('should be set semantics', async() => {
    store.addQuad(quad('s1', 'p1', 'o1'));
    store.addQuad(quad('s1', 'p1', 'o1'));

    store.removeQuad(quad('s2', 'p2', 'o2'));

    store.end();

    await expect(arrayifyStream(store.match())).resolves.toBeRdfIsomorphic([
      quad('s1', 'p1', 'o1'),
    ]);
  });
});
