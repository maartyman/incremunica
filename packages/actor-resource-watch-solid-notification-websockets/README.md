# Incremunica Solid Notifications Websockets Resource Watch Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-resource-watch-solid-notification-websockets.svg)](https://badge.fury.io/js/@incremunica%2Factor-resource-watch-solid-notification-websockets)

An incremunica Solid Notifications Websockets Resource Watch Actor.

## Install

```bash
$ yarn add @incremunica/actor-resource-watch-solid-notification-websockets
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-resource-watch-solid-notification-websockets/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:resource-watch/actors#actor-resource-watch-solid-notification-websockets",
      "@type": "ActorResourceWatchSolidNotificationWebsockets"
    }
  ]
}
```
