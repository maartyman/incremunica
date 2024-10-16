# Incremunica Incremental Minus Hash RDF Join Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-incremental-minus-hash.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-incremental-minus-hash)

An Incremunica Minus Hash RDF Join Actor.

## Install

```bash
$ yarn add @incremunica/actor-rdf-join-incremental-minus-hash
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-rdf-join-incremental-minus-hash/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-join/actors#incremental-minus-hash",
      "@type": "ActorRdfJoinIncrementalMinusHash",
      "mediatorJoinSelectivity": { "@id": "urn:comunica:default:rdf-join-selectivity/mediators#main" }
    }
  ]
}
```
