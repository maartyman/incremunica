# Incremunica Bindings Aggregator Factory Min Actor

[![npm version](https://badge.fury.io/js/%40incremunica%2Factor-bindings-aggregator-factory-min.svg)](https://www.npmjs.com/package/@incremunica/actor-bindings-aggregator-factory-min)

A [bindings aggregator factory](https://github.com/comunica/comunica/tree/master/packages/bus-bindings-aggregator-factory) actor
that constructs a bindings aggregator capable of evaluating [min](https://www.w3.org/TR/sparql11-query/#defn_aggMin).

## Install

```bash
$ yarn add @incremunica/actor-bindings-aggregator-factory-min
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-bindings-aggregator-factory-min/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:bindings-aggregator-factory/actors#min",
      "@type": "ActorBindingsAggregatorFactoryMin"
    }
  ]
}
```
