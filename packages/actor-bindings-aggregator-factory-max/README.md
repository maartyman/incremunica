# Incremunica Bindings Aggregator Factory Max Actor

[![npm version](https://badge.fury.io/js/%40incremunica%2Factor-bindings-aggregator-factory-max.svg)](https://www.npmjs.com/package/@incremunica/actor-bindings-aggregator-factory-max)

A [bindings aggregator factory](https://github.com/comunica/comunica/tree/master/packages/bus-bindings-aggregator-factory) actor
that constructs a bindings aggregator capable of evaluating [max](https://www.w3.org/TR/sparql11-query/#defn_aggMax).

## Install

```bash
$ yarn add @incremunica/actor-bindings-aggregator-factory-max
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-bindings-aggregator-factory-max/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:bindings-aggregator-factory/actors#max",
      "@type": "ActorBindingsAggregatorFactoryMax"
    }
  ]
}
```
