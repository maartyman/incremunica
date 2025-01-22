export type MatchOptions = {
  // Ends the result stream
  closeStream: () => void;
  // Deletes the result stream: first propagates everything as a deletion than closes the stream
  deleteStream: () => void;
};
