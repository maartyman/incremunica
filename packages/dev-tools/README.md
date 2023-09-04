# Incremunica development tools

[![npm version](https://badge.fury.io/js/@incremunica%2Fdev-tools.svg)](https://badge.fury.io/js/@incremunica%2Fdev-tools)

A collection of reusable incremunica Typescript functions for debugging.


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
