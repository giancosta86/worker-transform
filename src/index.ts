import { Transform, TransformCallback } from "node:stream";
import { cpus } from "node:os";
import { WorkerAgent } from "@giancosta86/worker-agent";
import { ErrorParts, formatError } from "@giancosta86/format-error";
import { Logger } from "@giancosta86/unified-logging";

export type ChunkInput<T> = Readonly<{
  value: T;
  encoding: BufferEncoding;
}>;

export type ChunkOutput<T> = Readonly<{
  value: T;
  encoding?: BufferEncoding;
}>;

type StreamAgent<TInput, TOutput> = WorkerAgent<
  ChunkInput<TInput>,
  ChunkOutput<TOutput>
>;

type WaitingChunk<TInput> = Readonly<{
  input: ChunkInput<TInput>;
  callback: TransformCallback;
}>;

export type WorkerTransformOptions = Readonly<{
  logger?: Logger;
  agentCount?: number;
  highWaterMark?: number;
  signal?: AbortSignal;
}>;

export class WorkerTransform<TInput, TOutput> extends Transform {
  private readonly agents: Set<StreamAgent<TInput, TOutput>>;
  private readonly freeAgents: StreamAgent<TInput, TOutput>[];

  private readonly logger?: Logger;

  private waitingChunk?: WaitingChunk<TInput>;

  private flushing = false;

  private flushingCallback?: TransformCallback;

  private errorEmitted = false;

  constructor(operationModuleId: string, options?: WorkerTransformOptions) {
    super({
      objectMode: true,
      highWaterMark: options?.highWaterMark,
      signal: options?.signal
    });

    this.logger = options?.logger;

    const agentCount = options?.agentCount ?? cpus().length;
    this.agents = new Set();

    for (let i = 0; i < agentCount; i++) {
      const agent = this.createAgent(operationModuleId);
      this.agents.add(agent);
    }

    this.freeAgents = [...this.agents];
  }

  private createAgent(operationModuleId: string): StreamAgent<TInput, TOutput> {
    const agent = new WorkerAgent<ChunkInput<TInput>, ChunkOutput<TOutput>>(
      operationModuleId
    )
      .on(
        "result",
        (err: Error | null, output: ChunkOutput<TOutput> | null) => {
          try {
            if (err) {
              this.logger?.warn(
                `Operation error: ${formatError(err, ErrorParts.Message)}`
              );
            } else {
              const { value, encoding } = output!;
              this.push(value, encoding);
            }
          } finally {
            this.notifyAgentAvailability(agent);
          }
        }
      )
      .on("error", err => {
        this.logger?.error(
          `Infrastructural error from worker: ${formatError(err)}`
        );

        if (!this.errorEmitted) {
          this.emit("error", err);
          this.errorEmitted = true;
        }
      })
      .on("exit", () => {
        this.logger?.debug("Agent exiting...");
        this.agents.delete(agent);

        const freeAgentIndex = this.freeAgents.indexOf(agent);
        if (freeAgentIndex > -1) {
          this.freeAgents.splice(freeAgentIndex, 1);
        }

        if (!this.agents.size && this.flushingCallback) {
          this.flushingCallback();
        }
      });

    return agent;
  }

  private notifyAgentAvailability(agent: StreamAgent<TInput, TOutput>): void {
    if (this.waitingChunk) {
      const chunk = this.waitingChunk;
      this.waitingChunk = undefined;

      agent.runOperation(chunk.input);
      chunk.callback();
      return;
    }

    if (this.flushing) {
      this.logger?.debug("Sending a termination request to the agent...");
      agent.requestExit();
      return;
    }

    this.logger?.debug("No task waiting: enqueuing the free agent...");
    this.freeAgents.push(agent);
  }

  override _transform(
    chunk: unknown,
    encoding: BufferEncoding,
    callback: TransformCallback
  ): void {
    const chunkInput: ChunkInput<TInput> = {
      value: chunk as TInput,
      encoding
    };

    const freeAgent = this.freeAgents.shift();
    if (!freeAgent) {
      this.waitingChunk = {
        input: chunkInput,
        callback
      };

      return;
    }

    freeAgent.runOperation(chunkInput);
    callback();
  }

  override _flush(callback: TransformCallback): void {
    this.logger?.debug("Starting the flushing phase...");
    this.flushing = true;

    this.flushingCallback = () => {
      this.logger?.debug("Ending the flushing phase...");
      callback();
    };

    this.freeAgents.forEach(agent => agent.requestExit());
  }

  override _destroy(
    error: Error | null,
    callback: (error: Error | null) => void
  ): void {
    this.logger?.debug("Destroying the stream...");
    this.agents.forEach(agent => agent.requestExit());

    super._destroy(error, callback);
  }
}
