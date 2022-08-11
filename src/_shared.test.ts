import { join } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { Logger } from "@giancosta86/unified-logging";
import { WorkerTransform } from ".";

export const CRASHING_VALUE = 999;

async function runAddingTransform(
  operationModuleBaseName: string,
  valueAddedByTheOperation: number,
  sourceItems: readonly number[],
  logger?: Logger
): Promise<void> {
  const expectedItems = sourceItems.flatMap(item =>
    item != CRASHING_VALUE ? [item + valueAddedByTheOperation] : []
  );

  const actualItems: unknown[] = [];

  const transform = new WorkerTransform(
    join(__dirname, operationModuleBaseName),
    {
      agentCount: 2,
      logger: logger ?? console
    }
  ).on("data", item => actualItems.push(item));

  await pipeline(Readable.from(sourceItems), transform);

  expect(new Set(actualItems)).toEqual(new Set(expectedItems));
}

export function runSyncTransform(
  sourceItems: readonly number[],
  logger?: Logger
): Promise<void> {
  return runAddingTransform("_add200.sync.test", 200, sourceItems, logger);
}

export function runAsyncTransform(
  sourceItems: readonly number[],
  logger?: Logger
): Promise<void> {
  return runAddingTransform("_add500.async.test", 500, sourceItems, logger);
}
