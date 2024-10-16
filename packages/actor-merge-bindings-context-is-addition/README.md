# Incremunica isAddition Merge Bindings Context Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-merge-bindings-context-is-addition.svg)](https://badge.fury.io/js/@incremunica%2Factor-merge-bindings-context-is-addition)

An incremunica Merge Bindings Context actor that merges the isAddition attribute.

## Install

```bash
$ yarn add @incremunica/actor-merge-bindings-context-is-addition
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-merge-bindings-context-is-addition/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:merge-bindings-context/actors#is-addition",
      "@type": "ActorMergeBindingsContextIsAddition"
    }
  ]
}
```
