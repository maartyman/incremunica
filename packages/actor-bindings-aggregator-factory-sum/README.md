# Incremunica Bindings Aggregator Factory Sum Actor

[![npm version](https://badge.fury.io/js/%40incremunica%2Factor-bindings-aggregator-factory-sum.svg)](https://www.npmjs.com/package/@incremunica/actor-bindings-aggregator-factory-sum)

A [bindings aggregator factory](https://github.com/comunica/comunica/tree/master/packages/bus-bindings-aggregator-factory) actor
that constructs a bindings aggregator capable of evaluating [sum](https://www.w3.org/TR/sparql11-query/#defn_aggSum).

## Install

```bash
$ yarn add @incremunica/actor-bindings-aggregator-factory-sum
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-bindings-aggregator-factory-sum/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:bindings-aggregator-factory/actors#sum",
      "@type": "ActorBindingsAggregatorFactorySum"
    }
  ]
}
```
