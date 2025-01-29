# Incremunica Naive Determine Changes Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-determine-changes-naive.svg)](https://badge.fury.io/js/@incremunica%2Factor-determine-changes-naive)

An incremunica Naive Determine Changes Actor.

## Install

```bash
$ yarn add @incremunica/actor-determine-changes-naive
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-determine-changes-naive/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:determine-changes/actors#naive",
      "@type": "ActorDetermineChangesNaive"
    }
  ]
}
```
