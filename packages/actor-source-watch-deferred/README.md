# Incremunica Deferred Source Watch Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-source-watch-deferred.svg)](https://badge.fury.io/js/@incremunica%2Factor-source-watch-deferred)

An incremunica Deferred Source Watch Actor.

## Install

```bash
$ yarn add @incremunica/actor-source-watch-deferred
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-source-watch-deferred/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:source-watch/actors#deferred",
      "@type": "ActorSourceWatchDeferred"
    }
  ]
}
```
