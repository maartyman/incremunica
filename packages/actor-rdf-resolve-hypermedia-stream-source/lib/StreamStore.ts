import {Term} from "@rdfjs/types";
import { Store } from "n3";
import {Stream, PassThrough, Duplex, Writable, Transform} from "readable-stream";
import * as RdfTerms from "rdf-terms";
import {Quad} from "@comunica/types/lib/Quad";
const streamifyArray = require('streamify-array');

export class StreamStore {
  private readonly store = new Store();
  private readonly triplePatterns: IndexedQuadPatterns = new IndexedQuadPatterns();

  constructor(stream?: Transform) {
    if (stream) {
      this.attachStream(stream);
    }
  }

  public copyOfStore(): Store {
    const newStore = new Store();
    for (const quad of this.store) {
      newStore.add(quad);
    }
    return newStore;
  }

  public attachStream(stream: Transform) {
    let other = this;
    let findBindingStream = new Writable({
      write(quad: Quad, encoding: BufferEncoding | string, callback: (error?: (Error | null)) => void) {
        //this gets executed when a new triple arrives in the stream
        //first, check if the triple is bindable to any of the subscribed triple patterns if so send it in that stream
        if (quad.diff == undefined) {
          quad.diff = true;
        }
        for (const stream of other.triplePatterns.get(quad)) {
          stream.write(quad);
        }
        //then, add or remove it to the store it to the store
        if (quad.diff) {
          other.store.add(quad);
        } else {
          other.store.delete(quad);
        }

        callback(null);
      },
      objectMode: true
    });
    stream.pipe(findBindingStream);
  }

  match(subject?: Term, predicate?: Term, object?: Term, graph?: Term): Stream {
    const storeResultStream = streamifyArray(this.store.getQuads(
      subject? subject : null,
      predicate? predicate : null,
      object? object : null,
      graph? graph : null
    ));

    const passThroughStream = new PassThrough({objectMode:true});

    this.triplePatterns.add(passThroughStream, subject, predicate, object, graph);

    return storeResultStream.pipe(passThroughStream, {end: false});
  }
}

class IndexedQuadPatterns {
  private triplePatternMap = new Map<string, Map<string, Map<string, Map<string, Duplex[]>>>>();

  private termToString(term?: Term) {
    return (term && term.termType !== "Variable")? term.value : "Variable";
  }

  add(passThroughStream: Duplex, subject?: Term, predicate?: Term, object?: Term, graph?: Term) {
    let subjectMap = this.triplePatternMap.get(this.termToString(subject));

    if(!subjectMap) {
      subjectMap = new Map<string, Map<string, Map<string, Duplex[]>>>()
        .set(this.termToString(predicate), new Map<string, Map<string, Duplex[]>>()
          .set(this.termToString(object), new Map<string, Duplex[]>()
            .set(this.termToString(graph), [passThroughStream])));
      this.triplePatternMap.set(this.termToString(subject), subjectMap);
      return;
    }

    let predicateMap = subjectMap.get(this.termToString(predicate));

    if(!predicateMap) {
      predicateMap = new Map<string, Map<string, Duplex[]>>()
          .set(this.termToString(object), new Map<string, Duplex[]>()
            .set(this.termToString(graph), [passThroughStream]));
      subjectMap.set(this.termToString(predicate), predicateMap);
      return;
    }

    let objectMap = predicateMap.get(this.termToString(object));

    if(!objectMap) {
      objectMap = new Map<string, Duplex[]>()
          .set(this.termToString(graph), [passThroughStream]);
      predicateMap.set(this.termToString(object), objectMap);
      return;
    }

    let graphMap = objectMap.get(this.termToString(graph));

    if(!graphMap) {
      objectMap.set(this.termToString(graph), [passThroughStream]);
      return;
    }

    graphMap.push(passThroughStream);
  }

  get(quad: Quad): Duplex[] {
    return this._get(RdfTerms.getTerms(quad), 0, this.triplePatternMap);
  }

  private _get(quad: Term[], index: number, map: Map<string, Map<string, any> | Duplex[]>) : Duplex[] {
    let map1 = map.get(quad[index].value);
    let map2 = map.get("Variable");

    let duplexes = new Array<Duplex>();
    if (index === 3) {
      if (map1) {
        duplexes.push(...<Duplex[]><any>map1);
      }
      if (map2) {
        duplexes.push(...<Duplex[]><any>map2);
      }
      return duplexes;
    }

    if (map1) {
      duplexes.push(...this._get(quad, index+1, <Map<string, any>><any>map1));
    }
    if (map2) {
      duplexes.push(...this._get(quad, index+1, <Map<string, any>><any>map2));
    }

    return duplexes;
  }
}
