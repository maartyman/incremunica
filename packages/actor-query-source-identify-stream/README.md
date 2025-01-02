# Incremunica Streaming hypermedia Query Source Identify Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-query-source-identify-stream.svg)](https://badge.fury.io/js/@incremunica%2Factor-query-source-identify-stream)

A [Query Source Identify](https://github.com/comunica/comunica/tree/master/packages/bus-query-source-identify) actor that handles [Streaming Hypermedia Sources](https://comunica.dev/docs/query/advanced/sources_querying/).

## Install

```bash
$ yarn add @incremunica/actor-query-source-identify-stream
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-query-source-identify-stream/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:query-source-identify/actors#stream",
      "@type": "ActorQuerySourceIdentifyStream",
      "mediatorQuerySourceIdentify": { "@id": "urn:comunica:default:query-source-identify/mediators#main" },
      "mediatorRdfMetadataAccumulate": { "@id": "urn:comunica:default:rdf-metadata-accumulate/mediators#main" }
    }
  ]
}
```
