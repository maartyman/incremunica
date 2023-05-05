# Comunica Incremental Types

A collection of reusable incremental Comunica Typescript interfaces and types.

This module is part of the [Comunica framework](https://github.com/comunica/comunica),
and should only be used by [developers that want to build their own query engine](https://comunica.dev/docs/modify/).

[Click here if you just want to query with Comunica](https://comunica.dev/docs/query/).

## Install

```bash
$ yarn add @comunica/incremental-types
```

## Usage

```typescript
import {Quad} from '@comunica/incremental-types';
import {BindingsFactory} from '@comunica/incremental-bindings-factory';

// ...

const quad: Quad = new Quad();
```

All types are available in [`index.ts`](https://github.com/comunica/comunica/blob/master/packages/context-entries/index.ts).
