import { QueryEngineBase } from '@comunica/actor-init-query';
import type { ActorInitQueryBase } from '@comunica/actor-init-query';
import type { IQueryContextCommon, QueryAlgebraContext, QueryStringContext } from '@incremunica/types';

// eslint-disable-next-line ts/no-require-imports,ts/no-var-requires,import/extensions
const engineDefault = require('../engine-default.js');

/**
 * An Incremunica SPARQL query engine.
 */
export class QueryEngine extends QueryEngineBase<IQueryContextCommon, QueryStringContext, QueryAlgebraContext> {
  public constructor(engine: ActorInitQueryBase = engineDefault()) {
    super(engine);
  }
}
