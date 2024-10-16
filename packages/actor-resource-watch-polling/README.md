# Incremunica Polling Resource Watch Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-resource-watch-polling.svg)](https://badge.fury.io/js/@incremunica%2Factor-resource-watch-polling)

An incremunica Polling Resource Watch Actor.

## Install

```bash
$ yarn add @incremunica/actor-resource-watch-polling
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-resource-watch-polling/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:resource-watch/actors#polling-diff",
      "@type": "ActorResourceWatchPolling"
    }
  ]
}
```
