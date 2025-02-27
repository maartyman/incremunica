# Incremunica OrderBy Query Operation Actor

[![npm version](https://badge.fury.io/js/%40incremunica%2Factor-query-operation-orderby.svg)](https://www.npmjs.com/package/@incremunica/actor-query-operation-orderby)

A [Query Operation](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation) actor that handles [SPARQL `ORDER BY`](https://www.w3.org/TR/sparql11-query/#sparqlOrderBy) operations.

## Install

```bash
$ yarn add @incremunica/actor-query-operation-orderby
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-query-operation-orderby/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:query-operation/actors#orderby",
      "@type": "ActorQueryOperationOrderBy",
      "mediatorQueryOperation": { "@id": "urn:comunica:default:query-operation/mediators#main" },
      "mediatorExpressionEvaluatorFactory": { "@id": "urn:comunica:default:expression-evaluator-factory/mediators#main" },
      "mediatorTermComparatorFactory": { "@id": "urn:comunica:default:term-comparator-factory/mediators#main" }
    }
  ]
}
```

### Config Parameters

* `mediatorQueryOperation`: A mediator over the [Query Operation bus](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation).
* `mediatorExpressionEvaluatorFactory`: A mediator over the [Expression Evaluator Factory bus](https://github.com/comunica/comunica/tree/master/packages/bus-expression-evaluator-factory).
* `mediatorTermComparatorFactory`: A factory to create a [Term Comparator Factory bus](https://github.com/comunica/comunica/tree/master/packages/bus-term-comparator-factory).
