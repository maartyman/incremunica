# Incremunica Stream None RDF Resolve Hypermedia Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-resolve-hypermedia-stream-none.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-resolve-hypermedia-stream-none)

A comunica Stream None RDF Resolve Hypermedia Actor.

## Install

```bash
$ yarn add @comunica/actor-rdf-resolve-hypermedia-stream-none
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-rdf-resolve-hypermedia-stream-none/^1.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-resolve-hypermedia/actors#stream-none",
      "@type": "ActorRdfResolveHypermediaStreamNone"
    }
  ]
}
```
