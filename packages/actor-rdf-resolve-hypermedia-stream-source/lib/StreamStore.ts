import {Quad, Term} from "@rdfjs/types";
import { Store } from "n3";
import {Stream, PassThrough, Duplex, Writable, Readable} from "readable-stream";

export class StreamStore {
  private readonly store = new Store();
  private readonly triplePatterns: indexedTriplePatterns = new indexedTriplePatterns();

  constructor(stream?: Readable) {
    if (stream) {
      this.attachStream(stream);
    }
  }

  public attachStream(stream: Readable) {
    let other = this;
    let findBindingStream = new Writable({
      write(chunk: Quad, encoding: BufferEncoding | string, callback: (error?: (Error | null)) => void) {
        //this gets executed when a new triple arrives in the stream
        //first, check if the triple is bindable to any of the subscribed bindings if so send it in that stream
        other.triplePatterns.get(chunk).write(chunk);
        //then, add it to the store
        other.store.add(chunk);
        callback(null);
      },
      objectMode: true
    });
    stream.pipe(findBindingStream)
  }

  match(subject?: Term, predicate?: Term, object?: Term, graph?: Term): Stream {
    // @ts-ignore
    const storeResultStream = <Stream><any>this.store.match(subject, predicate, object, graph);

    const passThroughStream = new PassThrough();

    this.triplePatterns.add(passThroughStream, subject, predicate, object, graph);

    return storeResultStream.pipe(passThroughStream, {end: false});
  }


}

class indexedTriplePatterns {
  //subjectMap = new Map<Term, Map<Term, Map<Term, Map<Term, Duplex>>>>();
  private subjectMap = new Array<{pattern:{subject?: Term, predicate?: Term, object?: Term, graph?: Term}, stream: Duplex}>();

  add(passThroughStream: Duplex, subject?: Term, predicate?: Term, object?: Term, graph?: Term) {
    //TODO indexTP
    this.subjectMap.push({pattern: {subject, predicate, object, graph} ,stream: passThroughStream});
  }

  get(quad: Quad): Duplex {
    //TODO return triplePattern
    this.subjectMap.forEach((object) => {
      if (object.pattern.subject && !(object.pattern.subject.equals(quad.subject))) {
        return
      }
      if (object.pattern.predicate && !(object.pattern.predicate.equals(quad.predicate))) {
        return
      }
      if (object.pattern.object && !(object.pattern.object.equals(quad.object))) {
        return
      }
      if (object.pattern.graph && !(object.pattern.graph.equals(quad.graph))) {
        return
      }
    })
    return new Duplex();
  }
}
