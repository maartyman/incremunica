import { Actor, Bus, IActorTest, Mediator} from '@comunica/core';
import { ActorGuardPolling } from '../lib/ActorGuardPolling';
import {IActionHttp, IActorHttpOutput } from "@comunica/bus-http";
import {IActionDereferenceRdf, IActorDereferenceRdfOutput} from "@comunica/bus-dereference-rdf";
import {ActorGuard, IActionGuard} from "@comunica/bus-guard";
import {Transform} from "readable-stream";
import arrayifyStream from "arrayify-stream";
import {DataFactory} from "rdf-data-factory";
import 'jest-rdf';
import {Store} from "n3";

const quad = require('rdf-quad');
const streamifyArray = require('streamify-array');


describe('ActorGuardPolling', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorGuardPolling instance', () => {
    let actor: ActorGuardPolling;
    let mediatorHttp: Mediator<
      Actor<IActionHttp, IActorTest, IActorHttpOutput>,
      IActionHttp, IActorTest, IActorHttpOutput>;
    let mediatorDereferenceRdf: Mediator<
      Actor<IActionDereferenceRdf, IActorTest, IActorDereferenceRdfOutput>,
      IActionDereferenceRdf, IActorTest, IActorDereferenceRdfOutput>;
    let action: IActionGuard;
    let etag = 0;
    let quadArray = [
      quad('s1', 'p1', 'o1'),
      quad('s2', 'p2', 'o2'),
    ];

    beforeEach(() => {
      etag = 0
      quadArray = [
        quad('s1', 'p1', 'o1'),
        quad('s2', 'p2', 'o2'),
      ];

      mediatorHttp = <any> {
        mediate: async(action: IActionHttp) => ({
          headers: {
            get: () => {
              return {
                age: undefined,
                'cache-control': undefined,
                etag: etag
              }
            }
          }
        }),
      };

      mediatorDereferenceRdf = <any> {
        mediate: async (action: IActionDereferenceRdf) => {
          return {
            data: streamifyArray(quadArray),
            headers: {
              age: undefined,
              'cache-control': undefined,
              etag: etag,
              forEach: (func: (val: any, key: any) => void) => {
                func('age', undefined);
                func('cache-control', undefined);
                func('etag', etag);
              }
            }
          };
        }
      }

      actor = new ActorGuardPolling({
        beforeActors: [],
        mediatorHttp: mediatorHttp,
        pollingFrequency: 1,
        name: 'actor', bus, mediatorDereferenceRdf
      });

      action = {
        context: <any> {},
        streamSource: <any> {
          store: {
            attachStream: (stream: Transform) => {

            },
            copyOfStore: () => {
              return new Store();
            }
          },
          source: {
            metadata: {
              age: undefined,
              'cache-control': undefined,
              etag: 0
            },
            url: "www.test.com"
          },
        }
      }

    });

    it('should test', () => {
      return expect(actor.test(<any> {})).resolves.toBeTruthy();
    });

    it('should attach a changes stream', async () => {
      action.streamSource.store.attachStream = async (stream: Transform) => {
        let array = await arrayifyStream(stream);

        console.log(JSON.stringify(array))

        expect(array).toBeRdfIsomorphic(quadArray);
      }

      await actor.run(action);
      await new Promise((resolve) => setTimeout((resolve) => resolve(), 1000, resolve));
      etag = 1;
      quadArray = [quad('s3', 'p3', 'o3')];
      await new Promise((resolve) => setTimeout((resolve) => resolve(), 1000, resolve));
      ActorGuard.deleteGuard(action.streamSource.source.url);
    });
  });
});
