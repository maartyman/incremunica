# Incremunica RDFjs Streaming Source RDF Resolve Quad Pattern Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-resolve-quad-pattern-rdfjs-streaming-source.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-resolve-quad-pattern-rdfjs-streaming-source)

A comunica RDFjs Streaming Source RDF Resolve Quad Pattern Actor.

## Install

```bash
$ yarn add @comunica/actor-rdf-resolve-quad-pattern-rdfjs-streaming-source
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-rdf-resolve-quad-pattern-rdfjs-streaming-source/^1.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-resolve-quad-pattern/actors#rdfjs-streaming-source",
      "@type": "ActorRdfResolveQuadPatternRdfjsStreamingSource"
    }
  ]
}
```
