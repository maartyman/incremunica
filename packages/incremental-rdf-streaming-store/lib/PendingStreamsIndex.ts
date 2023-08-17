import type * as RDF from '@rdfjs/types';
import { termToString } from 'rdf-string';
import { getTerms, QUAD_TERM_NAMES } from 'rdf-terms';
import type { Duplex } from 'readable-stream';

/**
 * A PendingStreamsIndex stores pending streams indexed by the quad pattern they have been created for.
 */
export class PendingStreamsIndex<Q extends RDF.BaseQuad = RDF.Quad> {
  private static readonly ID_VARIABLE = '?';
  private static readonly ID_SEPARATOR = ':';

  public readonly indexedStreams = new Map<string, Duplex[]>();

  protected termToString(term?: RDF.Term | null): string {
    return term && term.termType !== 'Variable' ? termToString(term) : PendingStreamsIndex.ID_VARIABLE;
  }

  /**
   * Add a new pending stream for the given quad pattern.
   * @param pendingStream A pending stream.
   * @param subject A term.
   * @param predicate A term.
   * @param object A term.
   * @param graph A term.
   */
  public addPatternListener(
    pendingStream: Duplex,
    subject?: RDF.Term | null,
    predicate?: RDF.Term | null,
    object?: RDF.Term | null,
    graph?: RDF.Term | null,
  ): void {
    // Append to index of pendingStreams
    const key = `${this.termToString(subject)}${PendingStreamsIndex.ID_SEPARATOR}${
      this.termToString(predicate)}${PendingStreamsIndex.ID_SEPARATOR}${
      this.termToString(object)}${PendingStreamsIndex.ID_SEPARATOR}${
      this.termToString(graph)}`;
    let existingListeners = this.indexedStreams.get(key);
    if (!existingListeners) {
      existingListeners = [];
      this.indexedStreams.set(key, existingListeners);
    }
    existingListeners.push(pendingStream);
  }

  public removeClosedPatternListener(
    subject?: RDF.Term | null,
    predicate?: RDF.Term | null,
    object?: RDF.Term | null,
    graph?: RDF.Term | null,
  ): void {
    // Append to index of pendingStreams
    const key = `${this.termToString(subject)}${PendingStreamsIndex.ID_SEPARATOR}${
      this.termToString(predicate)}${PendingStreamsIndex.ID_SEPARATOR}${
      this.termToString(object)}${PendingStreamsIndex.ID_SEPARATOR}${
      this.termToString(graph)}`;
    const existingListeners = this.indexedStreams.get(key);
    if (existingListeners !== undefined) {
      if (existingListeners.length < 2) {
        this.indexedStreams.delete(key);
        return;
      }
      for (let i = 0; i < existingListeners.length; i++) {
        if (existingListeners[i].closed) {
          existingListeners[i] = existingListeners[existingListeners.length - 1];
          existingListeners.pop();
        }
      }
    }
  }

  /**
   * Find all the pending streams from which their quad pattern match the given quad.
   * @param quad The quad to match patterns to.
   */
  public getPendingStreamsForQuad(quad: Q): Duplex[] {
    // Determine the combinations of quad patterns to look up
    let keys: string[][] = [ getTerms(quad).map(term => termToString(term)) ];
    for (let i = 0; i < QUAD_TERM_NAMES.length; i++) {
      const keysOld = keys;
      keys = [];
      for (const key of keysOld) {
        keys.push(key);
        const keyModified = [ ...key ];
        keyModified[i] = PendingStreamsIndex.ID_VARIABLE;
        keys.push(keyModified);
      }
    }

    // Fetch the pendingStreams for the quad pattern combinations
    const pendingStreams = [];
    for (const key of keys) {
      const found = this.indexedStreams.get(key.join(PendingStreamsIndex.ID_SEPARATOR));
      if (found) {
        pendingStreams.push(...found);
      }
    }
    return pendingStreams;
  }
}
