# Incremunica Context Entries

[![npm version](https://badge.fury.io/js/%40incremunica%2Fcontext-entries.svg)](https://www.npmjs.com/package/@incremunica/context-entries)

A collection of reusable Incremunica context key definitions.

## Install

```bash
$ yarn add @incremunica/context-entries
```

## Usage

```typescript
import { KeysInitSparql } from '@incremunica/context-entries';

// ...

const baseIRI = context.get(KeysInitSparql.baseIRI);
```

All available keys are available in [`Keys`](https://github.com/comunica/comunica/blob/master/packages/context-entries/lib/Keys.ts).
