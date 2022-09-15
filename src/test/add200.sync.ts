import { ChunkInput, ChunkOutput } from "..";
import { CRASHING_VALUE } from "./shared";

function add200({ value }: ChunkInput<number>): ChunkOutput<number> {
  if (value == CRASHING_VALUE) {
    throw new Error("Just a test error! ^__^!");
  }

  return { value: value + 200 };
}

export = add200;
