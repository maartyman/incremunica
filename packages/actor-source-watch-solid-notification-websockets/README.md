# Incremunica Solid Notifications Websockets Source Watch Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-source-watch-solid-notification-websockets.svg)](https://badge.fury.io/js/@incremunica%2Factor-source-watch-solid-notification-websockets)

An incremunica Solid Notifications Websockets Source Watch Actor.

## Install

```bash
$ yarn add @incremunica/actor-source-watch-solid-notification-websockets
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-source-watch-solid-notification-websockets/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:source-watch/actors#actor-source-watch-solid-notification-websockets",
      "@type": "ActorSourceWatchSolidNotificationWebsockets"
    }
  ]
}
```
