# Incremunica Incremental Quadpattern Query Operation Actor

[![npm version](https://badge.fury.io/js/@incremunica%2Factor-query-operation-incremental-quadpattern.svg)](https://badge.fury.io/js/@incremunica%2Factor-query-operation-incremental-quadpattern)

A [Query Operation](https://github.com/comunica/comunica/tree/master/packages/bus-query-operation) actor that handles [SPARQL triple/quad pattern](https://www.w3.org/TR/sparql11-query/#QSynTriples) operations
by delegating to the [RDF Resolve Quad Pattern bus](https://github.com/comunica/comunica/tree/master/packages/bus-rdf-resolve-quad-pattern).

This actor has been modified from the original [quad-pattern actor](https://github.com/comunica/comunica/tree/master/packages/actor-query-operation-quadpattern) to pass the state of the `diff` attribute from the quad to the binding.

## Install

```bash
$ yarn add @comunica/actor-query-operation-incremental-quadpattern
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-query-operation-incremental-quadpattern/^1.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:query-operation/actors#quadpattern",
      "@type": "ActorQueryOperationQuadpattern",
      "mediatorResolveQuadPattern": { "@id": "urn:comunica:default:query-operation/mediators#main" }
    }
  ]
}
```

### Config Parameters

* `mediatorResolveQuadPattern`: A mediator over the [RDF Resolve Quad Pattern bus](https://github.com/comunica/comunica/tree/master/packages/bus-rdf-resolve-quad-pattern).

## Notes

### Quad-pattern-level context

Optionally, quad pattern operations may have a `context` field
that is of type `ActionContext`.
If such a quad-pattern-level context is detected,
it will be merged with the actor operation context.

This feature is useful if you want to attach specific flags
to quad patterns within the query plan,
such as the source(s) it should query over.
