import {Actor, Bus, IActorTest, Mediator} from '@comunica/core';
import { ActorGuardPolling } from '../lib/ActorGuardPolling';
import {IActionHttp, IActorHttpOutput } from "@comunica/bus-http";
import {IActionDereferenceRdf, IActorDereferenceRdfOutput} from "@comunica/bus-dereference-rdf";

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

    beforeEach(() => {
      mediatorHttp = <any> {
        mediate: async() => ({}),
      };
      mediatorDereferenceRdf = <any> {
        mediate: async() => ({}),
      };
      actor = new ActorGuardPolling({ name: 'actor', bus, mediatorHttp, mediatorDereferenceRdf });
    });

    it('should test', () => {
      return;
    });
    //TODO make tests
  });
});
