# Incremunica Slice Ordered Query Operation Actor

[![npm version](https://badge.fury.io/js/%40incremunica%2Factor-query-operation-slice-ordered.svg)](https://www.npmjs.com/package/@incremunica/actor-query-operation-slice-ordered)

A [Query Operation](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation) actor that handles SPARQL [`OFFSET`](https://www.w3.org/TR/sparql11-query/#modOffset) and [`LIMIT`](https://www.w3.org/TR/sparql11-query/#modResultLimit) operations on ordered bindings streams.

## Install

```bash
$ yarn add @incremunica/actor-query-operation-slice-ordered
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@incremunica/actor-query-operation-slice-ordered/^2.0.0/components/context.jsonld"
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:query-operation/actors#slice-ordered",
      "@type": "ActorQueryOperationSliceOrdered",
      "mediatorQueryOperation": { "@id": "urn:comunica:default:query-operation/mediators#main" }
    }
  ]
}
```

### Config Parameters

* `mediatorQueryOperation`: A mediator over the [Query Operation bus](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation).
