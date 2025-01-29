# Incremunica Inner Nestedloop RDF Join Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-inner-nestedloop.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-join-inner-nestedloop)

A comunica Inner Nestedloop RDF Join Actor.

## Install

```bash
$ yarn add @comunica/actor-rdf-join-inner-nestedloop
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-rdf-join-inner-nestedloop/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-join/actors#inner-nestedloop",
      "@type": "ActorRdfJoinInnerNestedloop",
      "mediatorJoinSelectivity": { "@id": "urn:comunica:default:rdf-join-selectivity/mediators#main" }
    }
  ]
}
```
