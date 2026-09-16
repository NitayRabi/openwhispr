import type { AgentStreamChunk } from "./ReasoningService";

type RemoteMessage = {
  role: string;
  content: string | Array<Record<string, unknown>>;
  [key: string]: unknown;
};

export interface RemoteAgentStreamOptions {
  baseUrl: string;
  model: string;
  messages: RemoteMessage[];
  sessionId: string;
  apiKey?: string;
  signal?: AbortSignal;
  fetchImpl?: typeof fetch;
}

type PendingToolCall = { id: string; name: string; arguments: string };

export function normalizeRemoteAgentUrl(baseUrl: string): string {
  const trimmed = baseUrl.trim();
  if (!trimmed) throw new Error("Remote agent base URL is required");

  const url = new URL(trimmed);
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error("Remote agent URL must use HTTP or HTTPS");
  }
  const path = url.pathname.replace(/\/+$/, "");
  if (path.endsWith("/v1/chat/completions") || path.endsWith("/chat/completions")) {
    url.pathname = path;
  } else if (path.endsWith("/v1")) {
    url.pathname = `${path}/chat/completions`;
  } else {
    url.pathname = `${path}/v1/chat/completions`;
  }
  return url.toString();
}

async function readHttpError(response: Response): Promise<Error> {
  const body = await response.text();
  let detail = body.trim();
  try {
    const parsed = JSON.parse(body);
    detail = parsed?.error?.message || parsed?.message || detail;
  } catch {
    // Plain-text errors are useful as-is.
  }
  const suffix = detail ? `: ${detail}` : "";
  return new Error(
    `Remote agent request failed (${response.status} ${response.statusText})${suffix}`
  );
}

function appendToolCall(
  pending: Map<number, PendingToolCall>,
  delta: { index?: number; id?: string; function?: { name?: string; arguments?: string } }
): void {
  const index = delta.index ?? 0;
  const current = pending.get(index) ?? { id: "", name: "", arguments: "" };
  if (delta.id) current.id = delta.id;
  if (delta.function?.name) current.name += delta.function.name;
  if (delta.function?.arguments) current.arguments += delta.function.arguments;
  pending.set(index, current);
}

function normalizeCustomEvent(value: any): AgentStreamChunk | null {
  if (value?.type === "tool_result") {
    return {
      type: "tool_result",
      callId: String(value.callId ?? value.tool_call_id ?? ""),
      toolName: String(value.toolName ?? value.name ?? ""),
      displayText: String(value.displayText ?? value.result ?? value.output ?? "Done"),
      ...(value.metadata ? { metadata: value.metadata } : {}),
    };
  }
  if (value?.type === "content" && typeof value.text === "string") {
    return { type: "content", text: value.text };
  }
  return null;
}

/** Streams an OpenAI-compatible chat completion as the app's AgentStreamChunk shape. */
export async function* streamRemoteAgent(
  options: RemoteAgentStreamOptions
): AsyncGenerator<AgentStreamChunk, void, unknown> {
  const fetcher = options.fetchImpl ?? fetch;
  const headers: Record<string, string> = {
    Accept: "text/event-stream",
    "Content-Type": "application/json",
  };
  const apiKey = options.apiKey?.trim();
  if (apiKey) headers.Authorization = `Bearer ${apiKey}`;

  const response = await fetcher(normalizeRemoteAgentUrl(options.baseUrl), {
    method: "POST",
    headers,
    body: JSON.stringify({
      model: options.model,
      messages: options.messages,
      stream: true,
      user: options.sessionId,
    }),
    signal: options.signal,
  });

  if (!response.ok) throw await readHttpError(response);
  if (!response.body) throw new Error("Remote agent returned no response body");

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const pendingTools = new Map<number, PendingToolCall>();
  let buffer = "";
  let completed = false;

  const processData = function* (data: string): Generator<AgentStreamChunk> {
    if (data.trim() === "[DONE]") {
      completed = true;
      return;
    }
    let parsed: any;
    try {
      parsed = JSON.parse(data);
    } catch {
      return;
    }
    const custom = normalizeCustomEvent(parsed);
    if (custom) {
      yield custom;
      return;
    }
    for (const choice of parsed.choices ?? []) {
      const delta = choice.delta ?? {};
      if (typeof delta.content === "string" && delta.content) {
        yield { type: "content", text: delta.content };
      }
      for (const toolCall of delta.tool_calls ?? []) appendToolCall(pendingTools, toolCall);
      if (choice.finish_reason === "tool_calls" && pendingTools.size) {
        yield { type: "tool_calls", calls: [...pendingTools.values()] };
        pendingTools.clear();
      }
      if (choice.finish_reason && choice.finish_reason !== "tool_calls") {
        yield { type: "done", finishReason: choice.finish_reason };
        completed = true;
      }
    }
  };

  const processFrames = function* (flush = false): Generator<AgentStreamChunk> {
    const normalized = buffer.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
    const frames = normalized.split("\n\n");
    buffer = flush ? "" : (frames.pop() ?? "");
    for (const frame of flush ? frames : frames) {
      const data = frame
        .split("\n")
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice(5).replace(/^ /, ""))
        .join("\n");
      if (data) yield* processData(data);
    }
  };

  try {
    while (!completed) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      yield* processFrames();
    }
    buffer += decoder.decode();
    if (buffer.trim()) yield* processFrames(true);
    if (pendingTools.size) {
      yield { type: "tool_calls", calls: [...pendingTools.values()] };
      pendingTools.clear();
    }
    if (!completed) yield { type: "done", finishReason: "stop" };
  } finally {
    reader.releaseLock();
  }
}
