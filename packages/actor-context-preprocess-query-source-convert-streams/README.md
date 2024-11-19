# Comunica Query Source Convert Streams Context Preprocess Actor

[![npm version](https://badge.fury.io/js/%40comunica%2Factor-context-preprocess-query-source-convert-streams.svg)](https://www.npmjs.com/package/@comunica/actor-context-preprocess-query-source-convert-streams)

An [Context Preprocess](https://github.com/comunica/comunica/tree/master/packages/bus-context-preprocess) actor
that converts all streams in the query sources to AsyncIterators and puts them in an object.

[Click here if you just want to query with Comunica](https://comunica.dev/docs/query/).

## Install

```bash
$ yarn add @comunica/actor-context-preprocess-query-source-convert-streams
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-context-preprocess-query-source-convert-streams/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:context-preprocess/actors#query-source-convert-streams",
      "@type": "ActorContextPreprocessQuerySourceConvertStreams"
    }
  ]
}
```
