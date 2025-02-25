# Incremunica Bindings Aggregator Factory GroupConcat Actor

[![npm version](https://badge.fury.io/js/%40incremunica%2Factor-bindings-aggregator-factory-group-concat.svg)](https://www.npmjs.com/package/@incremunica/actor-bindings-aggregator-factory-group-concat)

A [bindings aggregator factory](https://github.com/comunica/comunica/tree/master/packages/bus-bindings-aggregator-factory) actor
that constructs a bindings aggregator capable of evaluating [group-concat](https://www.w3.org/TR/sparql11-query/#defn_aggGroupConcat).

## Install

```bash
$ yarn add @incremunica/actor-bindings-aggregator-factory-group-concat
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-bindings-aggregator-factory-group-concat/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:bindings-aggregator-factory/actors#group-concat",
      "@type": "ActorBindingsAggregatorFactoryGroupConcat"
    }
  ]
}
```
