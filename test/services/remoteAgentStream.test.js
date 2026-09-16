const test = require("node:test");
const assert = require("node:assert/strict");

const load = () => import("../../src/services/remoteAgentStream.ts");

function responseFromChunks(chunks, init = {}) {
  const encoder = new TextEncoder();
  return new Response(
    new ReadableStream({
      start(controller) {
        for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
        controller.close();
      },
    }),
    { status: 200, ...init }
  );
}

test("normalizes common base URL forms", async () => {
  const { normalizeRemoteAgentUrl } = await load();
  assert.equal(
    normalizeRemoteAgentUrl("https://agent.test"),
    "https://agent.test/v1/chat/completions"
  );
  assert.equal(
    normalizeRemoteAgentUrl("https://agent.test/v1/"),
    "https://agent.test/v1/chat/completions"
  );
  assert.equal(
    normalizeRemoteAgentUrl("https://agent.test/root/v1/chat/completions"),
    "https://agent.test/root/v1/chat/completions"
  );
  assert.throws(() => normalizeRemoteAgentUrl("file:///tmp/agent"), /HTTP or HTTPS/);
});

test("posts stable session and optional bearer auth and parses arbitrarily split SSE", async () => {
  const { streamRemoteAgent } = await load();
  let request;
  const fetchImpl = async (url, init) => {
    request = { url, init };
    return responseFromChunks([
      'data: {"choices":[{"delta":{"content":"Hel',
      'lo"}}]}\n\n:data comment\n\ndata: {"choices":[{"delta":{"content":"!"},"finish_reason":"stop"}]}\n\n',
    ]);
  };
  const chunks = [];
  for await (const chunk of streamRemoteAgent({
    baseUrl: "https://agent.test/v1",
    model: "agent-model",
    messages: [{ role: "user", content: "Hi" }],
    sessionId: "conversation-42",
    apiKey: " secret ",
    fetchImpl,
  }))
    chunks.push(chunk);

  assert.equal(request.url, "https://agent.test/v1/chat/completions");
  assert.equal(request.init.headers.Authorization, "Bearer secret");
  assert.deepEqual(JSON.parse(request.init.body), {
    model: "agent-model",
    messages: [{ role: "user", content: "Hi" }],
    stream: true,
    user: "conversation-42",
  });
  assert.deepEqual(chunks, [
    { type: "content", text: "Hello" },
    { type: "content", text: "!" },
    { type: "done", finishReason: "stop" },
  ]);
});

test("accumulates streamed tool call arguments", async () => {
  const { streamRemoteAgent } = await load();
  const frames = [
    {
      choices: [
        {
          delta: {
            tool_calls: [
              { index: 0, id: "call-1", function: { name: "search", arguments: '{"q":' } },
            ],
          },
        },
      ],
    },
    {
      choices: [
        {
          delta: { tool_calls: [{ index: 0, function: { arguments: '"cats"}' } }] },
          finish_reason: "tool_calls",
        },
      ],
    },
  ].map((value) => `data: ${JSON.stringify(value)}\n\n`);
  const output = [];
  for await (const chunk of streamRemoteAgent({
    baseUrl: "https://agent.test",
    model: "m",
    messages: [],
    sessionId: "s",
    fetchImpl: async () => responseFromChunks(frames),
  }))
    output.push(chunk);
  assert.deepEqual(output, [
    {
      type: "tool_calls",
      calls: [{ id: "call-1", name: "search", arguments: '{"q":"cats"}' }],
    },
    { type: "done", finishReason: "stop" },
  ]);
});

test("forwards signal and surfaces useful structured HTTP errors", async () => {
  const { streamRemoteAgent } = await load();
  const controller = new AbortController();
  let receivedSignal;
  const iterator = streamRemoteAgent({
    baseUrl: "https://agent.test",
    model: "m",
    messages: [],
    sessionId: "s",
    signal: controller.signal,
    fetchImpl: async (_url, init) => {
      receivedSignal = init.signal;
      return new Response(JSON.stringify({ error: { message: "bad model" } }), {
        status: 400,
        statusText: "Bad Request",
      });
    },
  });
  await assert.rejects(async () => iterator.next(), /400 Bad Request.*bad model/);
  assert.equal(receivedSignal, controller.signal);
});
