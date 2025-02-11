import 'jest';
import type { Bindings, BindingsStream } from '@comunica/types';
import { KeysBindings } from '@incremunica/context-entries';
import { BF, DF, partialArrayifyAsyncIterator } from '@incremunica/dev-tools';
import type { IQuerySourceStreamElement } from '@incremunica/types';
import { arrayifyStream } from 'arrayify-stream';
import { ArrayIterator, AsyncIterator } from 'asynciterator';
import { createSourcesStreamFromBindingsStream, QuerySourceIterator } from '../lib';

describe('SourcesTools', () => {
  describe('createSourcesStreamFromBindingsStream', () => {
    it('should return an iterator of IQuerySourceStreamElement', async() => {
      await expect(arrayifyStream(createSourcesStreamFromBindingsStream(new ArrayIterator<Bindings>([
        BF.fromRecord({
          s: DF.namedNode('http://ex.org/s'),
        }).setContextEntry(KeysBindings.isAddition, true),
      ])))).resolves.toEqual([
        { isAddition: true, querySource: 'http://ex.org/s' },
      ]);
    });

    it('should ignore non named nodes', async() => {
      await expect(arrayifyStream(createSourcesStreamFromBindingsStream(new ArrayIterator<Bindings>([
        BF.fromRecord({
          s: DF.namedNode('http://ex.org/s'),
        }).setContextEntry(KeysBindings.isAddition, true),
        BF.fromRecord({
          s: DF.variable('s'),
        }).setContextEntry(KeysBindings.isAddition, true),
        BF.fromRecord({
          s: DF.defaultGraph(),
        }).setContextEntry(KeysBindings.isAddition, true),
        BF.fromRecord({
          s: DF.literal('abc'),
        }).setContextEntry(KeysBindings.isAddition, true),
        BF.fromRecord({
          s: DF.quad(
            DF.namedNode('http://ex.org/q/s'),
            DF.namedNode('http://ex.org/q/p'),
            DF.namedNode('http://ex.org/q/o'),
          ),
        }).setContextEntry(KeysBindings.isAddition, true),
        BF.fromRecord({
          s: DF.blankNode('b1'),
        }).setContextEntry(KeysBindings.isAddition, true),
        BF.fromRecord({}).setContextEntry(KeysBindings.isAddition, true),
      ])))).resolves.toEqual([
        { isAddition: true, querySource: 'http://ex.org/s' },
      ]);
    });

    it('should work with deletions', async() => {
      await expect(arrayifyStream(createSourcesStreamFromBindingsStream(new ArrayIterator<Bindings>([
        BF.fromRecord({
          s: DF.namedNode('http://ex.org/s1'),
        }),
        BF.fromRecord({
          s: DF.namedNode('http://ex.org/s2'),
        }).setContextEntry(KeysBindings.isAddition, true),
        BF.fromRecord({
          s: DF.namedNode('http://ex.org/s3'),
        }).setContextEntry(KeysBindings.isAddition, false),
      ])))).resolves.toEqual([
        { isAddition: true, querySource: 'http://ex.org/s1' },
        { isAddition: true, querySource: 'http://ex.org/s2' },
        { isAddition: false, querySource: 'http://ex.org/s3' },
      ]);
    });

    it('should use the given variables', async() => {
      await expect(arrayifyStream(createSourcesStreamFromBindingsStream(new ArrayIterator<Bindings>([
        BF.fromRecord({
          v1: DF.namedNode('http://ex.org/1'),
          v2: DF.namedNode('http://ex.org/2'),
          v3: DF.namedNode('http://ex.org/3'),
        }),
        BF.fromRecord({
          v1: DF.namedNode('http://ex.org/4'),
          v2: DF.namedNode('http://ex.org/5'),
        }),
        BF.fromRecord({
          v1: DF.namedNode('http://ex.org/6'),
          v3: DF.namedNode('http://ex.org/7'),
        }),
        BF.fromRecord({
          v2: DF.namedNode('http://ex.org/8'),
          v3: DF.namedNode('http://ex.org/9'),
        }),
        BF.fromRecord({
          v1: DF.namedNode('http://ex.org/10'),
        }),
        BF.fromRecord({
          v2: DF.namedNode('http://ex.org/11'),
        }),
        BF.fromRecord({
          v3: DF.namedNode('http://ex.org/12'),
        }),
        BF.fromRecord({}),
      ]), [ 'v1', DF.variable('v2') ]))).resolves.toEqual([
        { isAddition: true, querySource: 'http://ex.org/1' },
        { isAddition: true, querySource: 'http://ex.org/2' },
        { isAddition: true, querySource: 'http://ex.org/4' },
        { isAddition: true, querySource: 'http://ex.org/5' },
        { isAddition: true, querySource: 'http://ex.org/6' },
        { isAddition: true, querySource: 'http://ex.org/8' },
        { isAddition: true, querySource: 'http://ex.org/10' },
        { isAddition: true, querySource: 'http://ex.org/11' },
      ]);
    });
  });

  describe('QuerySourceIterator', () => {
    let querySourcesIterator: QuerySourceIterator;
    let bindingsStream: BindingsStream;

    beforeEach(() => {
      bindingsStream = new ArrayIterator<Bindings>([
        BF.fromRecord({
          s: DF.namedNode('http://ex.org/s1'),
        }),
      ], { autoStart: false });
      bindingsStream.readable = false;
      querySourcesIterator = new QuerySourceIterator({
        bindingsStreams: [
          bindingsStream,
        ],
      });
    });

    afterEach(() => {
      querySourcesIterator.destroy();
      bindingsStream.destroy();
    });

    it('should not be readable if none of the iterators is readable', () => {
      const bindingsStream1 = new ArrayIterator<Bindings>([]);
      const bindingsStream2 = new ArrayIterator<Bindings>([]);
      const bindingsStream3 = new ArrayIterator<Bindings>([]);
      bindingsStream1.readable = false;
      bindingsStream2.readable = false;
      bindingsStream3.readable = false;
      querySourcesIterator = new QuerySourceIterator({
        bindingsStreams: [
          bindingsStream1,
          bindingsStream2,
          bindingsStream3,
        ],
      });
      expect(querySourcesIterator.readable).toBeFalsy();
    });

    it('should be readable if one of the iterators is readable', () => {
      const bindingsStream1 = new ArrayIterator<Bindings>([]);
      const bindingsStream2 = new ArrayIterator<Bindings>([]);
      const bindingsStream3 = new ArrayIterator<Bindings>([]);
      bindingsStream1.readable = false;
      bindingsStream2.readable = true;
      bindingsStream3.readable = false;
      querySourcesIterator = new QuerySourceIterator({
        bindingsStreams: [
          bindingsStream1,
          bindingsStream2,
          bindingsStream3,
        ],
      });
      expect(querySourcesIterator.readable).toBeTruthy();
    });

    it('should be destroyed if a source throws an error', async() => {
      await expect(new Promise<string>((_resolve, reject) => {
        querySourcesIterator.once('error', (error) => {
          reject(error);
        });
        bindingsStream.destroy(new Error('test error'));
      })).rejects.toThrow('test error');
      expect(querySourcesIterator.ended).toBeTruthy();
    });

    it('should become readable if one of the iterators becomes readable', async() => {
      querySourcesIterator.readable = false;
      expect(querySourcesIterator.readable).toBeFalsy();
      const sourceStream = new ArrayIterator([ <any>{} ]);
      sourceStream.readable = false;
      querySourcesIterator.addSourcesStream(sourceStream);
      expect(querySourcesIterator.readable).toBeFalsy();
      sourceStream.readable = true;
      await new Promise(resolve => setImmediate(resolve));
      expect(querySourcesIterator.readable).toBeTruthy();
    });

    it('should not end if all bindingsStream end', () => {
      const bindingsStream2 = new ArrayIterator<Bindings>([]);
      querySourcesIterator.addBindingsStream(bindingsStream2);

      bindingsStream.destroy();
      bindingsStream2.destroy();

      expect(bindingsStream.done).toBeTruthy();
      expect(bindingsStream2.done).toBeTruthy();
      expect(querySourcesIterator.ended).toBeFalsy();
    });

    it('should not end and if one bindingsStream ends', () => {
      const bindingsStream2 = new ArrayIterator<Bindings>([]);
      querySourcesIterator.addBindingsStream(bindingsStream2);
      const bindingsStream3 = new ArrayIterator<Bindings>([]);
      querySourcesIterator.addBindingsStream(bindingsStream3);

      bindingsStream.destroy();

      expect(bindingsStream.done).toBeTruthy();
      expect(bindingsStream2.done).toBeFalsy();
      expect(bindingsStream3.done).toBeFalsy();
      expect(querySourcesIterator.done).toBeFalsy();
    });

    it('should destroy iterators if end', () => {
      const sourceStream = new ArrayIterator([ <any>{} ]);
      sourceStream.readable = false;
      querySourcesIterator.addSourcesStream(sourceStream);
      querySourcesIterator._end();

      expect(bindingsStream.closed).toBeTruthy();
      expect(sourceStream.closed).toBeTruthy();
      expect(querySourcesIterator.closed).toBeTruthy();
    });

    it('should not addBindingsStream if ended', () => {
      querySourcesIterator._end();

      expect(bindingsStream.closed).toBeTruthy();
      expect(querySourcesIterator.closed).toBeTruthy();

      const bindingsStream2 = new ArrayIterator<Bindings>([]);
      expect(() => {
        querySourcesIterator.addBindingsStream(bindingsStream2);
      }).toThrow('Cannot add a bindings stream to a closed QuerySourceIterator');
      expect((<any>querySourcesIterator).sourcesStreams).toEqual([]);
    });

    it('should not addSourcesStream if ended', () => {
      querySourcesIterator._end();

      expect(bindingsStream.closed).toBeTruthy();
      expect(querySourcesIterator.closed).toBeTruthy();

      const sourcesStream = new ArrayIterator<IQuerySourceStreamElement>([]);
      expect(() => {
        querySourcesIterator.addSourcesStream(sourcesStream);
      }).toThrow('Cannot add a source stream to a closed QuerySourceIterator');
      expect((<any>querySourcesIterator).sourcesStreams).toEqual([]);
    });

    it('should not addSources if ended', () => {
      querySourcesIterator._end();

      expect(bindingsStream.closed).toBeTruthy();
      expect(querySourcesIterator.closed).toBeTruthy();

      expect(() => {
        querySourcesIterator.addSource('http://ex.org/s');
      }).toThrow('Cannot add a source to a closed QuerySourceIterator');
      expect((<any>querySourcesIterator).sources).toHaveLength(0);
    });

    it('should not removeSources if ended', () => {
      querySourcesIterator._end();

      expect(bindingsStream.closed).toBeTruthy();
      expect(querySourcesIterator.closed).toBeTruthy();

      expect(() => {
        querySourcesIterator.removeSource('http://ex.org/s');
      }).toThrow('Cannot remove a source to a closed QuerySourceIterator');
      expect((<any>querySourcesIterator).sources).toHaveLength(0);
    });

    it('should error and destroy all bindingsStreams if one bindingsStream errors', async() => {
      const bindingsStream2 = new ArrayIterator<Bindings>([]);
      querySourcesIterator.addBindingsStream(bindingsStream2);
      const bindingsStream3 = new ArrayIterator<Bindings>([]);
      querySourcesIterator.addBindingsStream(bindingsStream3);

      await expect(new Promise<string>((_resolve, reject) => {
        querySourcesIterator.on('error', (error) => {
          reject(error);
        });
        bindingsStream2.destroy(new Error('test error'));
      })).rejects.toThrow('test error');

      expect(bindingsStream.closed).toBeTruthy();
      expect(bindingsStream2.closed).toBeTruthy();
      expect(bindingsStream3.closed).toBeTruthy();
      expect(querySourcesIterator.closed).toBeTruthy();
    });

    it('should not read sources if no sources', async() => {
      querySourcesIterator = new QuerySourceIterator();
      expect(querySourcesIterator.read()).toBeNull();
      expect(querySourcesIterator.closed).toBeFalsy();
      expect(querySourcesIterator.readable).toBeFalsy();
    });

    it('should read sources', async() => {
      querySourcesIterator = new QuerySourceIterator({
        seedSources: [
          'http://ex.org/seed/1',
          'http://ex.org/seed/2',
        ],
        bindingsStreams: [
          new ArrayIterator<Bindings>([
            BF.fromRecord({
              s: DF.namedNode('http://ex.org/bs1/1'),
            }).setContextEntry(KeysBindings.isAddition, true),
            BF.fromRecord({
              s: DF.namedNode('http://ex.org/bs1/2'),
            }),
            BF.fromRecord({
              s: DF.namedNode('http://ex.org/bs1/1'),
            }).setContextEntry(KeysBindings.isAddition, false),
          ]),
          new ArrayIterator<Bindings>([
            BF.fromRecord({
              s: DF.namedNode('http://ex.org/bs2/1'),
            }),
          ]),
        ],
      });
      querySourcesIterator.addSource('http://ex.org/extra/1');
      querySourcesIterator.addSource('http://ex.org/extra/2');
      querySourcesIterator.removeSource('http://ex.org/extra/1');
      querySourcesIterator.addSourcesStream(new ArrayIterator<IQuerySourceStreamElement>([
        {
          isAddition: true,
          querySource: 'http://ex.org/ss1/1',
        },
        {
          isAddition: true,
          querySource: 'http://ex.org/ss1/2',
        },
        {
          isAddition: false,
          querySource: 'http://ex.org/ss1/1',
        },
      ]));
      querySourcesIterator.addBindingsStream(new ArrayIterator<Bindings>([
        BF.fromRecord({
          v1: DF.namedNode('http://ex.org/bs3/v1'),
          v2: DF.namedNode('http://ex.org/bs3/v2'),
          v3: DF.namedNode('http://ex.org/bs3/v3'),
        }),
      ]), [ 'v1', DF.variable('v2') ]);

      await new Promise<void>(resolve => setTimeout(resolve, 10));
      await expect(partialArrayifyAsyncIterator(querySourcesIterator, 14)).resolves.toEqual([
        {
          isAddition: true,
          querySource: 'http://ex.org/seed/1',
        },
        {
          isAddition: true,
          querySource: 'http://ex.org/seed/2',
        },
        {
          isAddition: true,
          querySource: 'http://ex.org/extra/1',
        },
        {
          isAddition: true,
          querySource: 'http://ex.org/extra/2',
        },
        {
          isAddition: false,
          querySource: 'http://ex.org/extra/1',
        },
        {
          isAddition: true,
          querySource: 'http://ex.org/bs2/1',
        },
        {
          isAddition: true,
          querySource: 'http://ex.org/ss1/1',
        },
        {
          isAddition: true,
          querySource: 'http://ex.org/bs3/v1',
        },
        {
          isAddition: true,
          querySource: 'http://ex.org/bs1/1',
        },
        {
          isAddition: true,
          querySource: 'http://ex.org/ss1/2',
        },
        {
          isAddition: true,
          querySource: 'http://ex.org/bs3/v2',
        },
        {
          isAddition: true,
          querySource: 'http://ex.org/bs1/2',
        },
        {
          isAddition: false,
          querySource: 'http://ex.org/ss1/1',
        },
        {
          isAddition: false,
          querySource: 'http://ex.org/bs1/1',
        },
      ]);
      expect(querySourcesIterator.read()).toBeNull();
      expect(querySourcesIterator.readable).toBeFalsy();
      expect(querySourcesIterator.closed).toBeFalsy();
    });

    it('should read sources slow sources', async() => {
      const iterator = new AsyncIterator<IQuerySourceStreamElement>();
      iterator.read = () => null;
      iterator.readable = true;
      querySourcesIterator = new QuerySourceIterator();
      querySourcesIterator.addSourcesStream(iterator);
      expect(querySourcesIterator.read()).toBeNull();
      expect(querySourcesIterator.readable).toBeFalsy();
      iterator.read = () => {
        if (iterator.readable) {
          iterator.readable = false;
          return {
            isAddition: true,
            querySource: 'http://ex.org/s',
          };
        }
        return null;
      };
      iterator.readable = true;
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(querySourcesIterator.readable).toBeTruthy();
      expect(querySourcesIterator.read()).toEqual({
        isAddition: true,
        querySource: 'http://ex.org/s',
      });
      expect(querySourcesIterator.readable).toBeTruthy();
      expect(querySourcesIterator.read()).toBeNull();
      expect(querySourcesIterator.readable).toBeFalsy();
    });

    describe('with lenient', () => {
      it('should not error and destroy all bindingsStreams if one bindingsStream errors', async() => {
        querySourcesIterator = new QuerySourceIterator({ lenient: true });
        const sourcesStream1 = new ArrayIterator<IQuerySourceStreamElement>([{
          isAddition: true,
          querySource: 'http://ex.org/s1',
        }], { autoStart: false });
        querySourcesIterator.addSourcesStream(sourcesStream1);
        const sourcesStream2 = new ArrayIterator<IQuerySourceStreamElement>([{
          isAddition: true,
          querySource: 'http://ex.org/s1',
        }], { autoStart: false });
        querySourcesIterator.addSourcesStream(sourcesStream2);
        const sourcesStream3 = new ArrayIterator<IQuerySourceStreamElement>([{
          isAddition: true,
          querySource: 'http://ex.org/s1',
        }], { autoStart: false });
        querySourcesIterator.addSourcesStream(sourcesStream3);

        expect(sourcesStream1.closed).toBeFalsy();
        expect(sourcesStream2.closed).toBeFalsy();
        expect(sourcesStream3.closed).toBeFalsy();
        expect(querySourcesIterator.closed).toBeFalsy();
        sourcesStream2.destroy(new Error('test error'));
        await new Promise(resolve => setImmediate(resolve));
        expect(sourcesStream1.closed).toBeFalsy();
        expect(sourcesStream2.closed).toBeTruthy();
        expect(sourcesStream3.closed).toBeFalsy();
        expect(querySourcesIterator.closed).toBeFalsy();
      });

      it('should not be destroyed if a source throws an error if lenient is true', async() => {
        bindingsStream = new ArrayIterator<Bindings>([
          BF.fromRecord({
            s: DF.namedNode('http://ex.org/s1'),
          }),
        ], { autoStart: false });
        querySourcesIterator = new QuerySourceIterator({ lenient: true, bindingsStreams: [ bindingsStream ]});
        bindingsStream.destroy(new Error('test error'));
        expect(querySourcesIterator.ended).toBeFalsy();
        expect((<any>querySourcesIterator).sourcesStreams).toHaveLength(0);
      });

      it('should not error on too many deletions', async() => {
        bindingsStream = new ArrayIterator<Bindings>([
          BF.fromRecord({
            s: DF.namedNode('http://ex.org/s1'),
          }).setContextEntry(KeysBindings.isAddition, false),
        ], { autoStart: false });
        querySourcesIterator = new QuerySourceIterator({
          bindingsStreams: [
            bindingsStream,
          ],
          lenient: true,
          distinct: true,
        });
        await expect(new Promise<void>((resolve, reject) => {
          querySourcesIterator.read();
          const countFunction = (): void => {
            try {
              let data = querySourcesIterator.read();
              while (data) {
                data = querySourcesIterator.read();
              }
              resolve();
            } catch (error: unknown) {
              reject(error);
            }
          };
          querySourcesIterator.on('readable', countFunction);
        })).resolves.toBeUndefined();
        expect(querySourcesIterator.closed).toBeFalsy();
        expect((<any>querySourcesIterator).sources).toHaveLength(0);
        expect((<any>querySourcesIterator).sourcesStreams).toHaveLength(1);
      });
    });

    describe('with district', () => {
      it('should remove duplicates', async() => {
        const sourceStream = new ArrayIterator<IQuerySourceStreamElement>([
          {
            isAddition: true,
            querySource: {
              type: 'hypermedia',
              value: 'http://ex.org/s1',
            },
          },
          {
            isAddition: true,
            querySource: 'http://ex.org/s1',
          },
          {
            isAddition: false,
            querySource: 'http://ex.org/s1',
          },
          {
            isAddition: false,
            querySource: 'http://ex.org/s1',
          },
        ], { autoStart: false });
        querySourcesIterator = new QuerySourceIterator({
          distinct: true,
        });
        querySourcesIterator.addSourcesStream(sourceStream);
        await expect(partialArrayifyAsyncIterator(querySourcesIterator, 2)).resolves.toEqual([
          {
            isAddition: true,
            querySource: {
              type: 'hypermedia',
              value: 'http://ex.org/s1',
            },
          },
          {
            isAddition: false,
            querySource: 'http://ex.org/s1',
          },
        ]);
      });

      it('should remove duplicates in single additions and removals', async() => {
        querySourcesIterator = new QuerySourceIterator({
          distinct: true,
        });
        querySourcesIterator.addSource({
          type: 'hypermedia',
          value: 'http://ex.org/s1',
        });
        querySourcesIterator.addSource('http://ex.org/s1');
        querySourcesIterator.addSource('http://ex.org/s1');
        querySourcesIterator.removeSource('http://ex.org/s1');
        querySourcesIterator.removeSource('http://ex.org/s1');
        querySourcesIterator.removeSource('http://ex.org/s1');
        await expect(partialArrayifyAsyncIterator(querySourcesIterator, 2)).resolves.toEqual([
          {
            isAddition: true,
            querySource: {
              type: 'hypermedia',
              value: 'http://ex.org/s1',
            },
          },
          {
            isAddition: false,
            querySource: 'http://ex.org/s1',
          },
        ]);
      });

      it('should ignore string sources in duplicates', async() => {
        const sourceStream = new ArrayIterator<IQuerySourceStreamElement>([
          {
            isAddition: true,
            querySource: '<http://ex.org/s1> <http://ex.org/p1> <http://ex.org/o1> .',
          },
          {
            isAddition: true,
            querySource: '<http://ex.org/s1> <http://ex.org/p1> <http://ex.org/o1> .',
          },
          {
            isAddition: false,
            querySource: '<http://ex.org/s1> <http://ex.org/p1> <http://ex.org/o1> .',
          },
          {
            isAddition: false,
            querySource: '<http://ex.org/s1> <http://ex.org/p1> <http://ex.org/o1> .',
          },
          {
            isAddition: false,
            querySource: '<http://ex.org/s1> <http://ex.org/p1> <http://ex.org/o1> .',
          },
        ], { autoStart: false });
        querySourcesIterator = new QuerySourceIterator({
          distinct: true,
        });
        querySourcesIterator.addSourcesStream(sourceStream);
        await expect(partialArrayifyAsyncIterator(querySourcesIterator, 2)).resolves.toEqual([
          {
            isAddition: true,
            querySource: '<http://ex.org/s1> <http://ex.org/p1> <http://ex.org/o1> .',
          },
          {
            isAddition: true,
            querySource: '<http://ex.org/s1> <http://ex.org/p1> <http://ex.org/o1> .',
          },
          {
            isAddition: false,
            querySource: '<http://ex.org/s1> <http://ex.org/p1> <http://ex.org/o1> .',
          },
          {
            isAddition: false,
            querySource: '<http://ex.org/s1> <http://ex.org/p1> <http://ex.org/o1> .',
          },
          {
            isAddition: false,
            querySource: '<http://ex.org/s1> <http://ex.org/p1> <http://ex.org/o1> .',
          },
        ]);
      });

      it('should not return the deletion if it is followed by the same addition', async() => {
        bindingsStream = new ArrayIterator<Bindings>([
          BF.fromRecord({
            s: DF.namedNode('http://ex.org/s1'),
          }),
          BF.fromRecord({
            s: DF.namedNode('http://ex.org/s1'),
          }).setContextEntry(KeysBindings.isAddition, false),
          BF.fromRecord({
            s: DF.namedNode('http://ex.org/s1'),
          }).setContextEntry(KeysBindings.isAddition, true),
          BF.fromRecord({
            s: DF.namedNode('http://ex.org/s2'),
          }).setContextEntry(KeysBindings.isAddition, true),
        ], { autoStart: false });
        querySourcesIterator = new QuerySourceIterator({
          bindingsStreams: [
            bindingsStream,
          ],
          distinct: true,
          deletionCooldown: 10,
        });
        await expect(partialArrayifyAsyncIterator(querySourcesIterator, 2)).resolves.toEqual([
          {
            isAddition: true,
            querySource: 'http://ex.org/s1',
          },
          {
            isAddition: true,
            querySource: 'http://ex.org/s2',
          },
        ]);
      });

      it('should error on too many deletions', async() => {
        bindingsStream = new ArrayIterator<Bindings>([
          BF.fromRecord({
            s: DF.namedNode('http://ex.org/s1'),
          }),
          BF.fromRecord({
            s: DF.namedNode('http://ex.org/s1'),
          }).setContextEntry(KeysBindings.isAddition, false),
          BF.fromRecord({
            s: DF.namedNode('http://ex.org/s1'),
          }).setContextEntry(KeysBindings.isAddition, false),
        ], { autoStart: false });
        querySourcesIterator = new QuerySourceIterator({
          bindingsStreams: [
            bindingsStream,
          ],
          distinct: true,
          deletionCooldown: 10,
        });
        await expect(new Promise<void>((_, reject) => {
          querySourcesIterator.read();
          const countFunction = (): void => {
            try {
              let data = querySourcesIterator.read();
              while (data) {
                data = querySourcesIterator.read();
              }
            } catch (error: unknown) {
              reject(error);
            }
          };
          querySourcesIterator.on('readable', countFunction);
        })).rejects.toThrow('Deleted source http://ex.org/s1 was never added');
      });
    });

    describe('with ignoreDeletions', () => {
      it('should ignore deletions', async() => {
        const sourceStream = new ArrayIterator<IQuerySourceStreamElement>([
          {
            isAddition: true,
            querySource: {
              type: 'hypermedia',
              value: 'http://ex.org/s1',
            },
          },
          {
            isAddition: true,
            querySource: 'http://ex.org/s1',
          },
          {
            isAddition: false,
            querySource: 'http://ex.org/s1',
          },
          {
            isAddition: false,
            querySource: 'http://ex.org/s1',
          },
          {
            isAddition: false,
            querySource: 'http://ex.org/s1',
          },
        ], { autoStart: false });
        querySourcesIterator = new QuerySourceIterator({
          ignoreDeletions: true,
        });
        querySourcesIterator.addSourcesStream(sourceStream);
        await expect(partialArrayifyAsyncIterator(querySourcesIterator, 2)).resolves.toEqual([
          {
            isAddition: true,
            querySource: {
              type: 'hypermedia',
              value: 'http://ex.org/s1',
            },
          },
          {
            isAddition: true,
            querySource: 'http://ex.org/s1',
          },
        ]);
      });
    });
  });
});
