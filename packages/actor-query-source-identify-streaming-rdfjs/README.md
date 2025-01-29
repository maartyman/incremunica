# Incremunica Streaming RDFJS Query Source Identify Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-query-source-identify-streaming-rdfjs.svg)](https://badge.fury.io/js/@incremunica%2Factor-query-source-identify-streaming-rdfjs)

A [Query Source Identify](https://github.com/comunica/comunica/tree/master/packages/bus-query-source-identify) actor that handles [Streaming RDF/JS Sources](https://comunica.dev/docs/query/advanced/rdfjs_querying/).

## Install

```bash
$ yarn add @incremunica/actor-query-source-identify-streaming-rdfjs
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-query-source-identify-streaming-rdfjs/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:query-source-identify/actors#streaming-rdfjs",
      "@type": "ActorQuerySourceIdentifyStreamingRdfJs",
      "mediatorMergeBindingsContext": { "@id": "urn:comunica:default:merge-bindings-context/mediators#main" }
    }
  ]
}
```
