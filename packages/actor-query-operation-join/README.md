# Incremunica Join Query Operation Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-query-operation-join.svg)](https://www.npmjs.com/package/@incremunica/actor-query-operation-join)

A [Query Operation](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation) actor that handles SPARQL Join operations
by delegating to the [RDF Join bus](https://github.com/comunica/comunica/tree/master/packages/bus-rdf-join) bus.

## Install

```bash
$ yarn add @incremunica/actor-query-operation-join
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-query-operation-join/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:query-operation/actors#join",
      "@type": "ActorQueryOperationJoin",
      "mediatorQueryOperation": { "@id": "urn:comunica:default:query-operation/mediators#main" },
      "mediatorJoin": { "@id": "urn:comunica:default:rdf-join/mediators#main" }
    }
  ]
}
```

### Config Parameters

* `mediatorQueryOperation`: A mediator over the [Query Operation bus](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation).
* `mediatorJoin`: A mediator over the [RDF Join bus](https://github.com/comunica/comunica/tree/master/packages/bus-rdf-join).
