# Incremunica Inner Incremental Delete Hash RDF Join Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-inner-incremental-delete-hash.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-inner-incremental-delete-hash)

A comunica Inner Incremental Delete Hash RDF Join Actor.

## Install

```bash
$ yarn add @comunica/actor-rdf-join-inner-incremental-delete-hash
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-rdf-join-inner-incremental-delete-hash/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-join/actors#inner-incremental-delete-hash",
      "@type": "ActorRdfJoinInnerIncrementalDeleteHash",
      "mediatorJoinSelectivity": { "@id": "urn:comunica:default:rdf-join-selectivity/mediators#main" }
    }
  ]
}
```
