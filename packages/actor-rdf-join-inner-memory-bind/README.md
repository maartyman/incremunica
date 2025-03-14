# Incremunica Inner Bind Join RDF Join Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-inner-memory-bind.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-inner-memory-bind)

A comunica Inner Bind RDF Join Actor.

## Install

```bash
$ yarn add @comunica/actor-rdf-join-inner-memory-bind
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-rdf-join-inner-memory-bind/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-join/actors#inner-memory-bind",
      "@type": "ActorRdfJoinBind",
      "mediatorJoinSelectivity": { "@id": "urn:comunica:default:rdf-join-selectivity/mediators#main" },
      "mediatorJoinEntriesSort": { "@id": "urn:comunica:default:rdf-join-entries-sort/mediators#main" },
      "mediatorQueryOperation": { "@id": "urn:comunica:default:query-operation/mediators#main" }
    }
  ]
}
```
