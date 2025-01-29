# Incremunica Minus Hash RDF Join Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-minus-hash.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-minus-hash)

An Incremunica Minus Hash RDF Join Actor.

## Install

```bash
$ yarn add @incremunica/actor-rdf-join-minus-hash
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-rdf-join-minus-hash/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-join/actors#minus-hash",
      "@type": "ActorRdfJoinMinusHash",
      "mediatorJoinSelectivity": { "@id": "urn:comunica:default:rdf-join-selectivity/mediators#main" }
    }
  ]
}
```
