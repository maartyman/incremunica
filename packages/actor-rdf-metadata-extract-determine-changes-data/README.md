# Incremunica Determine Changes Data RDF Metadata Extract Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-rdf-metadata-extract-determine-changes-data.svg)](https://badge.fury.io/js/@incremunica%2Factor-rdf-metadata-extract-determine-changes-data)

A comunica Determine Changes Data RDF Metadata Extract Actor.

## Install

```bash
$ yarn add @comunica/actor-rdf-metadata-extract-determine-changes-data
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-rdf-metadata-extract-determine-changes-data/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-metadata-extract/actors#determine-changes-data",
      "@type": "ActorRdfMetadataExtractDetermineChangesData"
    }
  ]
}
```
