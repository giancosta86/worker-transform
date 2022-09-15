import { ChunkInput, ChunkOutput } from "../WorkerTransform";
import { CRASHING_VALUE, NULLING_VALUE } from "./shared";

function add200({ value }: ChunkInput<number>): ChunkOutput<number | null> {
  if (value == CRASHING_VALUE) {
    throw new Error("Just a test error! ^__^!");
  }

  if (value == NULLING_VALUE) {
    return { value: null };
  }

  return { value: value + 200 };
}

export = add200;
