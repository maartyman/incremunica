# Incremunica Naive Guard Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-guard-naive.svg)](https://badge.fury.io/js/@incremunica%2Factor-guard-naive)

An incremunica Naive Guard Actor.

## Install

```bash
$ yarn add @incremunica/actor-guard-naive
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-guard-naive/^1.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:guard/actors#naive",
      "@type": "ActorGuardNaive"
    }
  ]
}
```
