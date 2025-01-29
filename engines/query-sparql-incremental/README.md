# Incremunica SPARQL

[![npm version](https://badge.fury.io/js/@incremunica%2Fquery-sparql-incremental.svg)](https://badge.fury.io/js/@incremunica%2Fquery-sparql-incremental)

Incremunica is an incremental SPARQL query engine build on top of [comunica](https://github.com/comunica/comunica).

## Install

```bash
$ npm install -g @incremunica/query-sparql-incremental
```

or

```bash
$ yarn add @incremunica/query-sparql-incremental
```

## Usage

Incremunica can be used in JavaScript/TypeScript applications and from the command line.

### Usage within application

This engine can be used in JavaScript/TypeScript applications as follows:

```javascript
const QueryEngine = require('@incremunica/query-sparql-incremental').QueryEngine;
const myEngine = new QueryEngine();

async function main() {
    const bindingsStream = await myEngine.queryBindings(`
    SELECT ?interest
    WHERE {
      <https://ruben.verborgh.org/profile/#me> foaf:topic_interest ?interest.
      <https://www.rubensworks.net/#me> foaf:topic_interest ?interest.
    }`, {
        sources: [
            "https://ruben.verborgh.org/profile/",
            "https://www.rubensworks.net/"
        ],
    });

    // Consume results as a stream
    bindingsStream.on('data', (binding) => {
        console.log("Is addition:", binding.diff); // If true: addition, if false: deletion.

        console.log(binding.toString()); // Quick way to print bindings for testing

        console.log("Has variable 'interest':", binding.has('interest')); // Will be true

        // Obtaining values
        console.log(binding.get('interest').value);
        console.log(binding.get('interest').termType);
    });

    bindingsStream.on('end', () => {
        // The data-listener will not be called anymore once we get here.
    });
    bindingsStream.on('error', (error) => {
        console.error(error);
    });

    // As this is an incremental query engine, you need to end the query yourself otherwise it will keep checking for changes.
    setTimeout(() => bindingsStream.close(), 3000);
}

main();
```

You can also use an [incremental triple store](https://www.npmjs.com/package/@incremunica/streaming-store).
This store allows you to change the dataset (additions and deletions) and show you the changes in the query results.
```javascript
const QueryEngine = require('@incremunica/query-sparql-incremental').QueryEngine;
const StreamingStore = require("@incremunica/streaming-store").StreamingStore;
const myEngine = new QueryEngine();
const streamingStore = new StreamingStore();

async function main() {
    streamingStore.import(quadStream);

    const bindingsStream = await myEngine.queryBindings(`
    SELECT *
    WHERE {
        ?s ?p ?o.
    }`, {
        sources: [ streamingStore ],
    });

    streamingStore.addQuad(quad);
    streamingStore.removeQuad(otherQuad);

    streamingStore.end();
}

main();
```

### Usage from the command line

Show the help with all options:

```bash
$ comunica-sparql-incremental --help
```

_[**Read more** about querying from the command line](https://comunica.dev/docs/query/getting_started/query_cli/)._
