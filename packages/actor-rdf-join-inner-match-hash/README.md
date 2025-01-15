# Incremunica Inner Match Hash RDF Join Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-inner-match-hash.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-inner-match-hash)

A comunica Inner Match Hash RDF Join Actor.

## Install

```bash
$ yarn add @comunica/actor-rdf-join-inner-match-hash
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-rdf-join-inner-match-hash/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-join/actors#inner-match-hash",
      "@type": "ActorRdfJoinInnerMatchHash",
      "mediatorJoinSelectivity": { "@id": "urn:comunica:default:rdf-join-selectivity/mediators#main" }
    }
  ]
}
```
