# Comunica SPARQL INCREMENTAL

<!--
[![npm version](https://badge.fury.io/js/%40comunica%2Fquery-sparql-ostrich.svg)](https://www.npmjs.com/package/@comunica/query-sparql-ostrich)
[![Docker Pulls](https://img.shields.io/docker/pulls/comunica/query-sparql-ostrich.svg)](https://hub.docker.com/r/comunica/query-sparql-ostrich/)
-->

Comunica SPARQL INCREMENTAL is a SPARQL query engine for JavaScript that enables incremental querying.

## Install

```bash
$ yarn add incremunica/query-sparql-incremental
```

or

```bash
$ npm install -g incremunica/query-sparql-incremental
```

## Install a prerelease

Since this package is still in testing phase, you may want to install a prerelease of this package, which you can do by appending `@next` to the package name during installation.

```bash
$ yarn add incremunica/query-sparql-incremental@next
```

or

```bash
$ npm install -g incremunica/query-sparql-incremental@next
```

## Usage

Show the help with all options:

```bash
$ comunica-sparql-incremental --help
```

_[**Read more** about querying from the command line](https://comunica.dev/docs/query/getting_started/query_cli/)._

### Usage within application

This engine can be used in JavaScript/TypeScript applications as follows:

```javascript
const QueryEngine = require('@comunica/query-sparql-incremental').QueryEngine;
const myEngine = new QueryEngine();

const bindingsStream = await myEngine.queryBindings(`
  SELECT * WHERE {
    GRAPH <version:1> {
      ?s ?p ?o
    }
  } LIMIT 100`, {
    lenient: true,
});

// Consume results as a stream (best performance)
bindingsStream.on('data', (binding) => {
    console.log(binding.toString()); // Quick way to print bindings for testing

    console.log(binding.has('s')); // Will be true

    // Obtaining values
    console.log(binding.get('s').value);
    console.log(binding.get('s').termType);
    console.log(binding.get('p').value);
    console.log(binding.get('o').value);
});
bindingsStream.on('end', () => {
    // The data-listener will not be called anymore once we get here.
});
bindingsStream.on('error', (error) => {
    console.error(error);
});

// Consume results as an array (easier)
const bindings = await bindingsStream.toArray();
console.log(bindings[0].get('s').value);
console.log(bindings[0].get('s').termType);
```

_[**Read more** about querying an application](https://comunica.dev/docs/query/getting_started/query_app/)._

### Usage as a SPARQL endpoint

The SPARQL endpoint can only be started dynamically.
An alternative config file can be passed via the `COMUNICA_CONFIG` environment variable.

Use `bin/http.js` when running in the Comunica monorepo development environment.

_[**Read more** about setting up a SPARQL endpoint](https://comunica.dev/docs/query/getting_started/setup_endpoint/)._
