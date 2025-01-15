# Incremunica Polling Source Watch Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-source-watch-polling.svg)](https://badge.fury.io/js/@incremunica%2Factor-source-watch-polling)

An incremunica Polling Source Watch Actor.

## Install

```bash
$ yarn add @incremunica/actor-source-watch-polling
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-source-watch-polling/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:source-watch/actors#polling",
      "@type": "ActorSourceWatchPolling"
    }
  ]
}
```
