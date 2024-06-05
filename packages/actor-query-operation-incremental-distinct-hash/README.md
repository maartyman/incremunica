# Incremunica Incremental Distinct Hash Query Operation Actor

[![npm version](https://badge.fury.io/js/%40Incremunica%2Factor-query-operation-incremental-distinct-hash.svg)](https://www.npmjs.com/package/@Incremunica/actor-query-operation-incremental-distinct-hash)

A [Query Operation](https://github.com/Incremunica/Incremunica/tree/master/packages/bus-query-operation) actor that handles [SPARQL `DISTINCT`](https://www.w3.org/TR/sparql11-query/#sparqlDistinct) operations
by maintaining a hash-based cache of infinite size.

This module is part of the [Incremunica framework](https://github.com/Incremunica/Incremunica),
and should only be used by [developers that want to build their own query engine](https://Incremunica.dev/docs/modify/).

[Click here if you just want to query with Incremunica](https://Incremunica.dev/docs/query/).

## Install

```bash
$ yarn add @Incremunica/actor-query-operation-incremental-distinct-hash
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@Incremunica/actor-query-operation-incremental-distinct-hash/^2.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "urn:Incremunica:default:query-operation/actors#distinct",
      "@type": "ActorQueryOperationIncrementalDistinctHash",
      "mediatorQueryOperation": { "@id": "#mediatorQueryOperation" }
    }
  ]
}
```

### Config Parameters

* `mediatorQueryOperation`: A mediator over the [Query Operation bus](https://github.com/Incremunica/Incremunica/tree/master/packages/bus-query-operation).
