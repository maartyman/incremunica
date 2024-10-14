# Incremunica Incremental Filter Query Operation Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-query-operation-incremental-filter.svg)](https://badge.fury.io/js/@incremunica%2Factor-query-operation-incremental-filter)

A [Query Operation](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation) actor that handles [SPARQL filter](https://www.w3.org/TR/sparql11-query/#evaluation) operations.

## Install

```bash
$ yarn add @incremunica/actor-query-operation-incremental-filter
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-query-operation-incremental-filter/^1.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:query-operation/actors#filter",
      "@type": "ActorQueryOperationIncrementalFilter",
      "mediatorQueryOperation": { "@id": "urn:comunica:default:query-operation/mediators#main" },
      "mediatorMergeBindingsContext": { "@id": "urn:comunica:default:merge-bindings-context/mediators#main" }
    }
  ]
}
```
