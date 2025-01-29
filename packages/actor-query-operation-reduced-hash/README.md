# Incremunica Reduced Hash Query Operation Actor

[![npm version](https://badge.fury.io/js/%40incremunica%2Factor-query-operation-reduced-hash.svg)](https://www.npmjs.com/package/@incremunica/actor-query-operation-reduced-hash)

A [Query Operation](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation) actor that handles [SPARQL `REDUCED`](https://www.w3.org/TR/sparql11-query/#sparqlReduced) operations
by maintaining a hash-based cache of a fixed size.

## Install

```bash
$ yarn add @incremunica/actor-query-operation-reduced-hash
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-query-operation-reduced-hash/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:query-operation/actors#reduced",
      "@type": "ActorQueryOperationReducedHash",
      "mediatorQueryOperation": { "@id": "urn:comunica:default:query-operation/mediators#main" },
      "mediatorHashBindings": { "@id": "urn:comunica:default:hash-bindings/mediators#main" }
    }
  ]
}
```

### Config Parameters

* `mediatorQueryOperation`: A mediator over the [Query Operation bus](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation).
* `mediatorHashBindings`: A mediator over the [Hash Bindings bus](https://github.com/comunica/comunica/tree/master/packages/bus-hash-bindings).
