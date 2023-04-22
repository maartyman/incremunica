# Comunica Incremental Federated RDF Resolve Quad Pattern Actor

[![npm version](https://badge.fury.io/js/%40comunica%2Factor-rdf-resolve-quad-pattern-incremental-federated.svg)](https://www.npmjs.com/package/@comunica/actor-rdf-resolve-quad-pattern-incremental-federated)

An [RDF Resolve Quad Pattern](https://github.com/comunica/comunica/tree/master/packages/bus-rdf-resolve-quad-pattern) actor
that handles [a federation of multiple sources](https://comunica.dev/docs/query/advanced/federation/),
and delegates resolving each source separately using the [RDF Resolve Quad Pattern bus](https://github.com/comunica/comunica/tree/master/packages/bus-rdf-resolve-quad-pattern).

This actor has been modified from the original [quad-pattern-incremental-federated actor](https://github.com/comunica/comunica/tree/master/packages/actor-rdf-resolve-quad-pattern-incremental-federated) to pass the state of the `diff` attribute from the quad to the binding.

This module is part of the [Comunica framework](https://github.com/comunica/comunica),
and should only be used by [developers that want to build their own query engine](https://comunica.dev/docs/modify/).

[Click here if you just want to query with Comunica](https://comunica.dev/docs/query/).

## Install

```bash
$ yarn add @comunica/actor-rdf-resolve-quad-pattern-incremental-federated
```

## Configure

After installing, this package can be added to your engine's configuration as follows:
```text
{
  "@context": [
    ...
    "https://linkedsoftwaredependencies.org/bundles/npm/@comunica/actor-rdf-resolve-quad-pattern-incremental-federated/^2.0.0/components/context.jsonld"  
  ],
  "actors": [
    ...
    {
      "@id": "urn:comunica:default:rdf-resolve-quad-pattern/actors#incremental-federated",
      "@type": "ActorRdfResolveQuadPatternIncrementalFederated",
      "mediatorResolveQuadPattern": { "@id": "urn:comunica:default:query-operation/mediators#main" }
    }
  ]
}
```

### Config Parameters

* `mediatorResolveQuadPattern`: A mediator over the [RDF Resolve Quad Pattern bus](https://github.com/comunica/comunica/tree/master/packages/bus-rdf-resolve-quad-pattern).
