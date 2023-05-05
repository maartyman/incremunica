# Incremunica development tools

A collection of reusable incremunica Typescript functions for debugging.

This module is part of the [Comunica framework](https://github.com/comunica/comunica),
and should only be used by [developers that want to build their own query engine](https://comunica.dev/docs/modify/).

[Click here if you just want to query with Comunica](https://comunica.dev/docs/query/).

## Install

```bash
$ yarn add @comunica/dev-tools
```

## Usage

```typescript
import {DevTools} from '@comunica/dev-tools';

// ...

DevTools.printBindings(bindings);
```

All types are available in [`index.ts`](https://github.com/comunica/comunica/blob/master/packages/context-entries/index.ts).
