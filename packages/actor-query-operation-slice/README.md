# Incremunica Slice Query Operation Actor

[![npm version](https://badge.fury.io/js/%40incremunica%2Factor-query-operation-slice.svg)](https://www.npmjs.com/package/@incremunica/actor-query-operation-slice)

A [Query Operation](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation) actor that handles SPARQL [`OFFSET`](https://www.w3.org/TR/sparql11-query/#modOffset) and [`LIMIT`](https://www.w3.org/TR/sparql11-query/#modResultLimit) operations.

## Install

```bash
$ yarn add @incremunica/actor-query-operation-slice
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-query-operation-slice/^1.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:query-operation/actors#slice",
      "@type": "ActorQueryOperationSlice",
      "mediatorQueryOperation": { "@id": "urn:comunica:default:query-operation/mediators#main" }
    }
  ]
}
```

### Config Parameters

* `mediatorQueryOperation`: A mediator over the [Query Operation bus](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation).
