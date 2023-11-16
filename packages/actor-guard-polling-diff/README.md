# Incremunica Polling Diff Guard Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-guard-polling-diff.svg)](https://badge.fury.io/js/@incremunica%2Factor-guard-polling-diff)

A comunica Polling Diff Guard Actor.

## Install

```bash
$ yarn add @incremunica/actor-guard-polling-diff
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-guard-polling-diff/^1.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:guard/actors#polling-diff",
      "@type": "ActorGuardPollingDiff"
    }
  ]
}
```
