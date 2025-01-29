# Incremunica Bindings Aggregator Factory WildcardCount Actor

[![npm version](https://badge.fury.io/js/%40incremunica%2Factor-bindings-aggregator-factory-wildcard-count.svg)](https://www.npmjs.com/package/@incremunica/actor-bindings-aggregator-factory-wildcard-count)

A [bindings aggregator factory](https://github.com/comunica/comunica/tree/master/packages/bus-bindings-aggregator-factory) actor
that constructs a bindings aggregator capable of evaluating [wildcard-count](https://www.w3.org/TR/sparql11-query/#defn_aggCount).

## Install

```bash
$ yarn add @incremunica/actor-bindings-aggregator-factory-wildcard-count
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-bindings-aggregator-factory-wildcard-count/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:bindings-aggregator-factory/actors#wildcard-count",
      "@type": "ActorBindingsAggregatorFactoryWildcardCount"
    }
  ]
}
```
