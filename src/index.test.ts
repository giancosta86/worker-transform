import { ArrayLogger } from "@giancosta86/unified-logging";
import { join } from "node:path";
import { WorkerTransform } from ".";
import {
  CRASHING_VALUE,
  runAsyncTransform,
  runSyncTransform
} from "./test/shared";

describe("Worker transform stream", () => {
  it("should support destruction right after creation", () => {
    const transform = new WorkerTransform(
      join(__dirname, "test", "add200.sync")
    );

    transform.destroy();
  });

  describe("when passing an inexisting operation module", () => {
    it("should emit an error event", () =>
      new Promise<void>((resolve, reject) => {
        new WorkerTransform(join(__dirname, "INEXISTING")).on("error", err => {
          try {
            expect(err.message).toMatch(/^Cannot find module/);
          } catch (err) {
            return reject(err);
          }

          resolve();
        });
      }));
  });

  it("should support manual control", () =>
    new Promise<void>((resolve, reject) => {
      const transform = new WorkerTransform(
        join(__dirname, "test", "add200.sync")
      ).on("data", item => {
        try {
          expect(item).toBe(290);
          transform.end();
        } catch (err) {
          return reject(err);
        }

        resolve();
      });

      transform.write(90);
    }));

  describe.each([
    ["a synchronous", runSyncTransform],
    ["an asynchronous", runAsyncTransform]
  ])("when running %s transform", (_label, transform) => {
    it("should support a data-less pipeline", () => transform([]));

    it("should process a single value", () => transform([90]));

    it("should process more values than its number of agents", () =>
      transform([90, 92, 95, 98]));

    it("should process several more values than its number of agents", () =>
      transform(Array.from(Array(80).keys())));

    it("should ignore but log operation errors", async () => {
      const logger = new ArrayLogger();

      await transform(
        [
          20,
          CRASHING_VALUE,
          4,
          CRASHING_VALUE,
          80,
          CRASHING_VALUE,
          50,
          CRASHING_VALUE,
          CRASHING_VALUE,
          CRASHING_VALUE,
          CRASHING_VALUE,
          CRASHING_VALUE,
          5,
          7,
          32
        ],
        logger
      );

      const operationErrorRegex = /^Operation error:/;
      const operationErrorLogs = logger.warnMessages.filter(message =>
        operationErrorRegex.test(message)
      );

      expect(operationErrorLogs.length).toBe(8);
    });
  });
});
