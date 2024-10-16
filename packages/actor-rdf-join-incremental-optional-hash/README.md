# Incremunica Incremental Optional Hash RDF Join Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-incremental-optional-hash.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-incremental-optional-hash)

An Incremunica Optional Hash RDF Join Actor.

## Install

```bash
$ yarn add @incremunica/actor-rdf-join-incremental-optional-hash
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-rdf-join-incremental-optional-hash/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-join/actors#incremental-optional-hash",
      "@type": "ActorRdfJoinIncrementalOptionalHash",
      "mediatorJoinSelectivity": { "@id": "urn:comunica:default:rdf-join-selectivity/mediators#main" }
    }
  ]
}
```
