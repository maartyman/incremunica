# incremunica

### This package is currently under active development. Proper functionality is not guaranteed.

This package is currently an extension to the comunica query engine. 
It recomputes the query bindings when the sources change.
Future work will try to incorporate this behaviour into the comunica query engines as actors.
Also instead of recomputing the query bindings it will use incremental query techniques to incrementally update the results.
 
```
npm i incremunica
```

See [solid-aggregator-client](https://github.com/maartyman/solid-aggregator-client) and [solid-aggregator-server](https://github.com/maartyman/solid-aggregator-server) for implementations of this extension of the comunica query engine.
