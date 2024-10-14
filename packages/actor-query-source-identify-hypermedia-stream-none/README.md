# Incremunica Stream None Query Source Identify Hypermedia Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-query-source-identify-hypermedia-stream-none.svg)](https://badge.fury.io/js/@incremunica%2Factor-query-source-identify-hypermedia-stream-none)

A Query Source Identify Hypermedia actor that handles raw RDF files.

## Install

```bash
$ yarn add @incremunica/actor-query-source-identify-hypermedia-stream-none
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-query-source-identify-hypermedia-stream-none/^1.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:query-source-identify-hypermedia/actors#stream-none",
      "@type": "ActorQuerySourceIdentifyHypermediaStreamNone",
      "mediatorGuard": { "@id": "urn:comunica:default:guard/mediators#main"  },
      "mediatorMergeBindingsContext": { "@id": "urn:comunica:default:merge-bindings-context/mediators#main" }
    }
  ]
}
```
