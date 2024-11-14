# Incremunica Inner Incremental Bind Join RDF Join Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-inner-incremental-memory-bind.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-inner-incremental-memory-bind)

A comunica Inner Incremental Bind RDF Join Actor.

## Install

```bash
$ yarn add @comunica/actor-rdf-join-inner-incremental-memory-bind
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-rdf-join-inner-incremental-memory-bind/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-join/actors#inner-incremental-memory-bind",
      "@type": "ActorRdfJoinIncrementalBind",
      "mediatorJoinSelectivity": { "@id": "urn:comunica:default:rdf-join-selectivity/mediators#main" },
      "mediatorJoinEntriesSort": { "@id": "urn:comunica:default:rdf-join-entries-sort/mediators#main" },
      "mediatorQueryOperation": { "@id": "urn:comunica:default:query-operation/mediators#main" }
    }
  ]
}
```
