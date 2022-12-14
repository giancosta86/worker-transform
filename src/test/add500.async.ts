import { setTimeout as delay } from "node:timers/promises";
import { ChunkInput, ChunkOutput } from "../WorkerTransform";
import { CRASHING_VALUE, NULLING_VALUE } from "./shared";

async function add500({
  value
}: ChunkInput<number>): Promise<ChunkOutput<number | null>> {
  await delay(5);
  await delay(2);
  await delay(6);

  if (value == CRASHING_VALUE) {
    throw new Error("Just a test error! ^__^!");
  }

  if (value == NULLING_VALUE) {
    return { value: null };
  }

  return { value: value + 500 };
}

export = add500;
