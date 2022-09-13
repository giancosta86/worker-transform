import { setTimeout as delay } from "node:timers/promises";
import { ChunkInput, ChunkOutput } from ".";
import { CRASHING_VALUE } from "./_shared.test";

async function add500({
  value
}: ChunkInput<number>): Promise<ChunkOutput<number>> {
  if (value == CRASHING_VALUE) {
    throw new Error("Just a test error! ^__^!");
  }

  await delay(5);
  await delay(2);
  await delay(6);

  return Promise.resolve({ value: value + 500 });
}

export = add500;
