# Incremunica None Query Source Identify Hypermedia Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-query-source-identify-hypermedia-none.svg)](https://badge.fury.io/js/@incremunica%2Factor-query-source-identify-hypermedia-none)

A Query Source Identify Hypermedia actor that handles raw RDF files.

## Install

```bash
$ yarn add @incremunica/actor-query-source-identify-hypermedia-none
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-query-source-identify-hypermedia-none/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:query-source-identify-hypermedia/actors#none",
      "@type": "ActorQuerySourceIdentifyHypermediaNone",
      "mediatorGuard": { "@id": "urn:comunica:default:determine-changes/mediators#main"  },
      "mediatorMergeBindingsContext": { "@id": "urn:comunica:default:merge-bindings-context/mediators#main" }
    }
  ]
}
```
