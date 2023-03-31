import { Bus } from '@comunica/core';
import { ActorRdfResolveHypermediaStreamNone } from '../lib/ActorRdfResolveHypermediaStreamNone';
import {MediatorGuard} from "@comunica/bus-guard";
import {DataFactory} from "rdf-data-factory";
import arrayifyStream from "arrayify-stream";
import 'jest-rdf'

const DF = new DataFactory();
const quad = require('rdf-quad');
const streamifyArray = require('streamify-array');

describe('ActorRdfResolveHypermediaStreamNone', () => {
  let bus: any;

  beforeEach(() => {
    bus = new Bus({ name: 'bus' });
  });

  describe('An ActorRdfResolveHypermediaStreamNone instance', () => {
    let actor: ActorRdfResolveHypermediaStreamNone;
    let mediatorGuard: MediatorGuard;

    beforeEach(() => {
      mediatorGuard = <any> {
        mediated: false,
        mediate: () => {
          (<any>mediatorGuard).mediated = true;
        }
      };
      actor = new ActorRdfResolveHypermediaStreamNone({ name: 'actor', bus, mediatorGuard});
    });

    it('should test', async () => {
      let action = <any>{};
      expect(await actor.test(action)).toMatchObject({filterFactor: 0});
    });

    it('should run and make a streaming store', async () => {
      let action = <any> {
        context: {
          get: () => {
            return ""
          }
        },
        url: "http://test.com",
        quads: streamifyArray([
          quad("s1","p1","o1"),
          quad("s2","p2","o2")
        ])
      };
      let stream = (await actor.run(action)).source.match(
        DF.variable('s'),
        DF.variable('p'),
        DF.variable('o'),
        DF.variable('g'),
      )
      let number = 2
      stream.on("data", () => {
        number--;
        if (number == 0) {
          stream.close();
        }
      })
      expect(await arrayifyStream(stream)).toBeRdfIsomorphic([
        quad("s1","p1","o1"),
        quad("s2","p2","o2")
      ]);
    });

    it('should run and add a guard', async () => {
      let action = <any> {
        context: {
          get: () => {
            return ""
          }
        },
        url: "http://test.com",
        quads: streamifyArray([])
      };
      await actor.run(action)
      expect((<any>mediatorGuard).mediated).toBeTruthy()
    });
  });
});
