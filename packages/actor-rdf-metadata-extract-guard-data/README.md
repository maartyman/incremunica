# Incremunica Guard Data RDF Metadata Extract Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-metadata-extract-guard-data.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-metadata-extract-guard-data)

A comunica Guard Data RDF Metadata Extract Actor.

## Install

```bash
$ yarn add @comunica/actor-rdf-metadata-extract-guard-data
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-rdf-metadata-extract-guard-data/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-metadata-extract/actors#guard-data",
      "@type": "ActorRdfMetadataExtractGuardData"
    }
  ]
}
```
