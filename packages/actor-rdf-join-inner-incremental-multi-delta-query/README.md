# Incremunica Inner Incremental Multi Delta Query RDF Join Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-inner-incremental-multi-delta-query.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-inner-incremental-multi-delta-query)

A comunica Inner Incremental Multi Delta Query RDF Join Actor.

## Install

```bash
$ yarn add @comunica/actor-rdf-join-inner-incremental-multi-delta-query
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-rdf-join-inner-incremental-multi-delta-query/^1.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-join/actors#inner-incremental-multi-delta-query",
      "@type": "ActorRdfJoinInnerIncrementalNestedloop",
      "mediatorJoinSelectivity": { "@id": "urn:comunica:default:rdf-join-selectivity/mediators#main" }
    }
  ]
}
```
