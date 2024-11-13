# Incremunica Deferred Resource Watch Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-resource-watch-deferred.svg)](https://badge.fury.io/js/@incremunica%2Factor-resource-watch-deferred)

An incremunica Deferred Resource Watch Actor.

## Install

```bash
$ yarn add @incremunica/actor-resource-watch-deferred
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-resource-watch-deferred/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:resource-watch/actors#deferred",
      "@type": "ActorResourceWatchDeferred"
    }
  ]
}
```
