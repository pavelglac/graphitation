import {
  ApolloClient,
  ApolloLink,
  NetworkStatus,
  Observable,
  gql,
} from "@apollo/client";
import type { ApolloQueryResult, FetchResult } from "@apollo/client";
import { GraphQLError, parse } from "graphql";
import {
  executeWithSchema,
  isIncrementalExecutionResult,
} from "@graphitation/supermassive";
import { ForestRun } from "../ForestRun";
import type { ExecutionPatchResult as IncrementalResponse } from "@apollo/client/link/core/types";

const query = gql`
  query {
    messages {
      id
      text
      ... @defer {
        name
      }
    }
  }
`;
const first = { __typename: "Message", id: "1", text: "original" };
const second = { __typename: "Message", id: "2", text: "new", name: "second" };
function writer(cache: ForestRun, document = query, token = {}) {
  return (result: IncrementalResponse) => {
    cache.batch({
      optimistic: false,
      update() {
        if (result.data) {
          cache.write({
            query: document,
            result: result.data,
            defer: {
              token,
              pending: "pending" in result ? result.pending : undefined,
            },
          });
        }
        for (const patch of result.incremental ?? []) {
          cache.write({
            query: document,
            result: patch.data,
            defer: { token, path: patch.path },
          });
        }
      },
    });
    if (!result.hasNext) cache.releaseIncremental(token);
  };
}
const initial: IncrementalResponse = {
  data: { messages: [first] },
  pending: [{ path: ["messages", 0] }],
  hasNext: true,
};
const final: IncrementalResponse = {
  incremental: [{ path: ["messages", 0], data: { name: "Alice" } }],
  hasNext: false,
};

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("Apollo pending-path integration", () => {
  test("does not add a promise hop to ordinary result processing", async () => {
    const document = gql`
      query {
        message {
          id
          text
        }
      }
    `;
    let send!: (result: FetchResult) => void;
    const client = new ApolloClient({
      cache: new ForestRun(),
      link: new ApolloLink(
        () =>
          new Observable((observer) => {
            send = (result) => observer.next(result);
          }),
      ),
    });
    const manager = client["queryManager"];
    const info = manager["getOrCreateQuery"]("ordinary-result");
    info.init({
      document,
      variables: {},
      networkStatus: NetworkStatus.loading,
    });
    const next = jest.fn();
    const subscription = manager["getResultsFromLink"](
      info,
      2 /* CacheWriteBehavior.MERGE */,
      {
        query: document,
        variables: {},
        context: {},
        fetchPolicy: "network-only",
        errorPolicy: "none",
      },
    ).subscribe(next);
    try {
      send({ data: { message: first } });
      await Promise.resolve();
      expect(next).toHaveBeenCalledWith({
        data: { message: first },
        loading: false,
        networkStatus: NetworkStatus.ready,
      });
    } finally {
      subscription.unsubscribe();
      client.stop();
    }
  });

  test("applies a multi-patch frame in the existing query transaction", async () => {
    let send!: (result: FetchResult) => void;
    const seen = deferred<void>();
    const finished = deferred<ApolloQueryResult<{ messages: unknown[] }>>();
    const cache = new ForestRun();
    const other = { ...first, id: "2" };
    const client = new ApolloClient({
      cache,
      link: new ApolloLink(
        () =>
          new Observable((observer) => {
            send = (result) => observer.next(result);
          }),
      ),
    });
    const subscription = client
      .watchQuery<{ messages: unknown[] }>({
        query,
        fetchPolicy: "network-only",
      })
      .subscribe({
        next(result) {
          if (result.loading && result.data?.messages?.length) seen.resolve();
          if (!result.loading) finished.resolve(result);
        },
      });
    send({
      data: { messages: [first, other] },
      pending: [{ path: ["messages", 0] }, { path: ["messages", 1] }],
      hasNext: true,
    });
    await seen.promise;
    const notify = jest.fn();
    const unwatch = cache.watch({ query, optimistic: false, callback: notify });
    const batch = jest.spyOn(cache, "batch");
    const transaction = jest.spyOn(cache, "performTransaction");
    try {
      send({
        incremental: [
          { path: ["messages", 0], data: { name: "Alice" } },
          { path: ["messages", 1], data: { name: "Bob" } },
        ],
        hasNext: false,
      });
      const expected = {
        messages: [
          { ...first, name: "Alice" },
          { ...other, name: "Bob" },
        ],
      };
      expect((await finished.promise).data).toEqual(expected);
      expect(transaction).toHaveBeenCalledTimes(1);
      expect(batch).not.toHaveBeenCalled();
      expect(notify).toHaveBeenCalledTimes(1);
      expect(notify.mock.calls[0][0].result).toEqual(expected);
    } finally {
      batch.mockRestore();
      transaction.mockRestore();
      unwatch();
      subscription.unsubscribe();
      client.stop();
    }
  });

  test.each(["network-only", "no-cache"] as const)(
    "consumes Supermassive without restoring changed lists (%s)",
    async (fetchPolicy) => {
      const name = deferred<string>();
      const initialSeen = deferred<void>();
      const finished =
        deferred<ApolloQueryResult<{ messages: (typeof first)[] }>>();
      const cache = new ForestRun();
      const client = new ApolloClient({
        cache,
        link: new ApolloLink(
          (operation) =>
            new Observable((observer) => {
              let cancelled = false;
              let iterator: AsyncGenerator<FetchResult, void, void> | undefined;
              (async () => {
                const execution = await executeWithSchema({
                  definitions: parse(
                    `type Query { messages: [Message!]! } type Message { id: ID!, text: String, name: String }`,
                  ),
                  document: operation.query,
                  rootValue: { messages: [first] },
                  resolvers: { Message: { name: () => name.promise } },
                });
                if (!isIncrementalExecutionResult(execution))
                  throw new Error("Expected deferred execution");
                iterator = execution.subsequentResults;
                if (cancelled) {
                  await iterator.return();
                  return;
                }
                observer.next(execution.initialResult);
                for await (const payload of iterator) {
                  if (!cancelled) observer.next(payload);
                }
                if (!cancelled) observer.complete();
              })().catch((error) => observer.error(error));
              return () => {
                cancelled = true;
                void iterator?.return();
              };
            }),
        ),
      });
      const sub = client
        .watchQuery<{ messages: (typeof first)[] }>({ query, fetchPolicy })
        .subscribe({
          next(result) {
            if (result.loading && result.data?.messages?.length)
              initialSeen.resolve();
            if (!result.loading) finished.resolve(result);
          },
          error(error) {
            throw error;
          },
        });
      await initialSeen.promise;
      expect(cache.getStats().treeCount).toBe(
        fetchPolicy === "no-cache" ? 0 : 1,
      );
      cache.writeQuery({ query, data: { messages: [second, first] } });
      name.resolve("Alice");
      const result = await finished.promise;
      expect(result.data.messages).toEqual(
        fetchPolicy === "no-cache"
          ? [{ ...first, name: "Alice" }]
          : [second, { ...first, name: "Alice" }],
      );
      expect(cache["deferredWrites"].size).toBe(0);
      sub.unsubscribe();
      client.stop();
    },
  );

  test("releases placeholders when a pending request is cancelled", async () => {
    let send!: (result: FetchResult) => void;
    const seen = deferred<void>();
    const cache = new ForestRun();
    const client = new ApolloClient({
      cache,
      link: new ApolloLink(
        () =>
          new Observable((observer) => {
            send = (result) => observer.next(result);
          }),
      ),
    });
    const sub = client
      .watchQuery({ query, fetchPolicy: "network-only" })
      .subscribe({
        next(result) {
          if (result.data?.messages?.length) seen.resolve();
        },
      });
    send(initial);
    await seen.promise;
    expect(cache["deferredWrites"].size).toBe(1);
    sub.unsubscribe();
    expect(cache["deferredWrites"].size).toBe(0);
    client.stop();
  });

  test("resolves client fields after remote patches without rewriting remote data", async () => {
    const document = gql`
      query {
        messages {
          id
          text
          title @client
          ... @defer {
            name
          }
        }
      }
    `;
    let send!: (result: FetchResult) => void;
    const seen = deferred<void>();
    const finished = deferred<ApolloQueryResult<{ messages: unknown[] }>>();
    const cache = new ForestRun();
    const client = new ApolloClient({
      cache,
      resolvers: {
        Message: {
          title: (parent, _args, context) => {
            expect(context.getCacheKey(parent)).toBe(`Message:${parent.id}`);
            return parent.name || parent.text;
          },
        },
      },
      link: new ApolloLink(
        () =>
          new Observable((observer) => {
            send = (result) => observer.next(result);
          }),
      ),
    });
    const sub = client
      .watchQuery<{ messages: unknown[] }>({
        query: document,
        fetchPolicy: "network-only",
      })
      .subscribe({
        next(result) {
          if (result.loading && result.data?.messages?.length) seen.resolve();
          if (!result.loading) finished.resolve(result);
        },
      });
    send(initial);
    await seen.promise;
    cache.writeQuery({ query: document, data: { messages: [second, first] } });
    send(final);
    expect((await finished.promise).data.messages).toEqual([
      { ...second, title: "second" },
      { ...first, name: "Alice", title: "Alice" },
    ]);
    sub.unsubscribe();
    client.stop();
  });
});
describe("incremental request lifecycle", () => {
  test("deduplicates writes while applying each subscriber's error policy", async () => {
    let send!: (result: FetchResult) => void;
    const request = jest.fn(
      () =>
        new Observable<FetchResult>((observer) => {
          send = (result) => observer.next(result);
        }),
    );
    const name = jest.fn((_existing, incoming) => incoming);
    const cache = new ForestRun({
      typePolicies: { Message: { fields: { name: { merge: name } } } },
    });
    const client = new ApolloClient({ cache, link: new ApolloLink(request) });
    const seen = deferred<void>();
    const failed = deferred<Error>();
    const finished = deferred<ApolloQueryResult<{ messages: unknown[] }>>();
    const strict = client
      .watchQuery({ query, fetchPolicy: "network-only", errorPolicy: "none" })
      .subscribe({
        error: failed.resolve,
      });
    const tolerant = client
      .watchQuery<{ messages: unknown[] }>({
        query,
        fetchPolicy: "network-only",
        errorPolicy: "all",
      })
      .subscribe({
        next(result) {
          if (result.loading && result.data?.messages?.length) seen.resolve();
          if (!result.loading) finished.resolve(result);
        },
      });
    expect(request).toHaveBeenCalledTimes(1);
    send(initial);
    await seen.promise;
    send({
      hasNext: false,
      incremental: [
        {
          path: ["messages", 0],
          data: { name: "Alice" },
          errors: [new GraphQLError("partial")],
        },
      ],
    });
    expect((await failed.promise).message).toContain("partial");
    const result = await finished.promise;
    expect(result.data.messages).toEqual([{ ...first, name: "Alice" }]);
    expect(result.errors?.[0].message).toBe("partial");
    expect(name).toHaveBeenCalledTimes(1);
    expect(cache["deferredWrites"].size).toBe(0);
    strict.unsubscribe();
    tolerant.unsubscribe();
    client.stop();
  });

  test("refetch replaces the old execution's bindings", async () => {
    const sources: { next(result: FetchResult): void; complete(): void }[] = [];
    const seen = deferred<void>();
    const cache = new ForestRun();
    const client = new ApolloClient({
      cache,
      link: new ApolloLink(
        () =>
          new Observable((observer) => {
            sources.push({
              next: (result) => observer.next(result),
              complete: () => observer.complete(),
            });
          }),
      ),
    });
    const observable = client.watchQuery({
      query,
      fetchPolicy: "network-only",
    });
    const subscription = observable.subscribe({
      next(result) {
        if (result.loading && result.data?.messages?.length) seen.resolve();
      },
    });
    sources[0].next(initial);
    await seen.promise;
    const refetch = observable.refetch();
    expect(sources).toHaveLength(2);
    expect(cache["deferredWrites"].size).toBe(0);
    sources[0].next(final);
    sources[1].next({
      ...initial,
      data: { messages: [{ ...first, id: "2" }] },
    });
    sources[1].next(final);
    sources[1].complete();
    expect((await refetch).data.messages).toEqual([
      { ...first, id: "2", name: "Alice" },
    ]);
    expect(cache["deferredWrites"].size).toBe(0);
    subscription.unsubscribe();
    client.stop();
  });

  test("leaves client-only queries on the ordinary resolver path", async () => {
    const cache = new ForestRun();
    const client = new ApolloClient({
      cache,
      link: ApolloLink.empty(),
      resolvers: { Query: { local: () => "local value" } },
    });
    const result = await client.query({
      query: gql`
        query {
          ... on Query @client @defer {
            local @client
          }
        }
      `,
      fetchPolicy: "network-only",
    });
    expect(result.data).toMatchObject({ local: "local value" });
    expect(cache["deferredWrites"].size).toBe(0);
    client.stop();
  });

  test("does not substitute cached data for a null error response", async () => {
    const cache = new ForestRun();
    cache.writeQuery({ query, data: { messages: [second] } });
    const client = new ApolloClient({
      cache,
      link: new ApolloLink(() =>
        Observable.of({
          data: null,
          hasNext: false,
          errors: [new GraphQLError("failed")],
        }),
      ),
    });
    const result = await client.query({
      query,
      fetchPolicy: "network-only",
      errorPolicy: "all",
    });
    expect(result.data).toBeNull();
    expect(cache.readQuery({ query })).toEqual({ messages: [second] });
    client.stop();
  });
});

describe("server-announced incremental writes", () => {
  test("releases a failed initial write so its token can be reused", () => {
    const cache = new ForestRun();
    const token = {};
    expect(() =>
      cache.write({
        query,
        result: { messages: [first] },
        defer: { token, pending: [{ path: ["missing"] }] },
      }),
    ).toThrow("Pending path does not identify an object");
    expect(cache["deferredWrites"].has(token)).toBe(false);
    const write = writer(cache, query, token);
    write(initial);
    write(final);
    expect(cache.readQuery({ query })).toEqual({
      messages: [{ ...first, name: "Alice" }],
    });
  });

  test("releases only the execution whose direct patch write fails", () => {
    const cache = new ForestRun();
    const failedToken = {};
    const liveToken = {};
    writer(cache, query, failedToken)(initial);
    const liveWrite = writer(cache, query, liveToken);
    liveWrite(initial);
    expect(() =>
      cache.write({
        query,
        result: { name: "wrong" },
        defer: { token: failedToken, path: ["missing"] },
      }),
    ).toThrow("Deferred path does not identify an original target");
    expect(cache["deferredWrites"].has(failedToken)).toBe(false);
    expect(cache["deferredWrites"].has(liveToken)).toBe(true);
    liveWrite(final);
    expect(cache.readQuery({ query })).toEqual({
      messages: [{ ...first, name: "Alice" }],
    });
  });

  test("preserves an own __proto__ response alias when preparing a patch", () => {
    const document = gql`
      query {
        messages {
          id
          ... @defer {
            __proto__: name
          }
        }
      }
    `;
    const message = Object.assign(Object.create(null), {
      __typename: "Message",
      id: "1",
    });
    const cache = new ForestRun();
    const write = writer(cache, document);
    cache.writeQuery({ query, data: { messages: [first] } });
    write({ ...initial, data: { messages: [message] } });
    write({
      ...final,
      incremental: [
        { path: ["messages", 0], data: JSON.parse('{"__proto__":"Alice"}') },
      ],
    });
    expect(cache.readQuery({ query })).toEqual({
      messages: [{ ...first, name: "Alice" }],
    });
  });

  test.each(["prepend", "remove"])(
    "preserves original identities after %s",
    (change) => {
      const cache = new ForestRun();
      const write = writer(cache);
      write(initial);
      cache.writeQuery({
        query,
        data: { messages: change === "prepend" ? [second, first] : [second] },
      });
      write(final);
      expect(cache.readQuery({ query })).toEqual({
        messages:
          change === "prepend"
            ? [second, { ...first, name: "Alice" }]
            : [second],
      });
      expect(cache.getStats().treeCount).toBe(1);
      expect(cache["deferredWrites"].size).toBe(0);
    },
  );

  test("binds an absolute patch path below a pending fragment to its original target", () => {
    const cache = new ForestRun();
    const write = writer(cache);
    write({ ...initial, pending: [{ path: [] }] });
    cache.writeQuery({ query, data: { messages: [second, first] } });
    write(final);
    expect(cache.readQuery({ query })).toEqual({
      messages: [second, { ...first, name: "Alice" }],
    });
  });

  test("does not rewrite relationships or rerun their merge policies for ancestor payloads", () => {
    const messages = jest.fn((_existing, incoming) => incoming);
    const name = jest.fn((_existing, incoming) => incoming);
    const cache = new ForestRun({
      typePolicies: {
        Query: { fields: { messages: { merge: messages } } },
        Message: { fields: { name: { merge: name } } },
      },
    });
    const write = writer(cache);
    write({ ...initial, pending: [{ path: [] }] });
    cache.writeQuery({ query, data: { messages: [second, first] } });
    const notify = jest.fn();
    const unwatch = cache.watch({ query, optimistic: false, callback: notify });
    messages.mockClear();
    name.mockClear();
    write({
      ...final,
      incremental: [{ path: [], data: { messages: [{ name: "Alice" }] } }],
    });
    expect(cache.readQuery({ query })).toEqual({
      messages: [second, { ...first, name: "Alice" }],
    });
    expect(messages).not.toHaveBeenCalled();
    expect(name).toHaveBeenCalledTimes(1);
    expect(notify).toHaveBeenCalledTimes(1);
    unwatch();
  });

  test("regular writes bind nested list positions before rows move", () => {
    const document = gql`
      query {
        rows {
          id
          text
          ... @defer {
            name
          }
        }
      }
    `;
    const other = { __typename: "Message", id: "2", text: "other" };
    const cache = new ForestRun();
    const token = {};
    cache.write({
      query: document,
      result: { rows: [[first], [other]] },
      defer: {
        token,
        pending: [{ path: ["rows", 0, 0] }, { path: ["rows", 1, 0] }],
      },
    });
    cache.writeQuery({ query: document, data: { rows: [[other], [first]] } });
    cache.batch({
      optimistic: false,
      update() {
        cache.write({
          query: document,
          result: { name: "Alice" },
          defer: { token, path: ["rows", 0, 0] },
        });
        cache.write({
          query: document,
          result: { name: "Bob" },
          defer: { token, path: ["rows", 1, 0] },
        });
      },
    });
    cache.releaseIncremental(token);
    expect(cache.readQuery({ query: document })).toEqual({
      rows: [[{ ...other, name: "Bob" }], [{ ...first, name: "Alice" }]],
    });
    expect(cache.getStats().treeCount).toBe(1);
  });

  test("keeps placeholders when indexing repairs shared source objects", () => {
    const document = gql`
      query {
        left: message {
          id
          ... @defer {
            name
          }
        }
        right: message {
          id
          ... @defer {
            name
          }
        }
      }
    `;
    const message = { __typename: "Message", id: "1" };
    const cache = new ForestRun();
    const token = {};
    cache.write({
      query: document,
      result: { left: message, right: message },
      defer: { token, pending: [{ path: ["left"] }, { path: ["right"] }] },
    });
    cache.write({
      query: document,
      result: { name: "Alice" },
      defer: { token, path: ["right"] },
    });
    cache.releaseIncremental(token);
    expect(cache.readQuery({ query: document })).toEqual({
      left: { ...message, name: "Alice" },
      right: { ...message, name: "Alice" },
    });
  });

  test("keeps existing cached values visible while awaiting the server", () => {
    const cache = new ForestRun();
    cache.writeQuery({
      query,
      data: { messages: [{ ...first, name: "cached" }] },
    });
    const write = writer(cache);
    write(initial);
    expect(cache.readQuery({ query })).toEqual({
      messages: [{ ...first, name: "cached" }],
    });
    write(final);
    expect(cache.readQuery({ query })).toEqual({
      messages: [{ ...first, name: "Alice" }],
    });
  });

  test("registers another execution even when the initial response object is reused", () => {
    const cache = new ForestRun();
    writer(cache)(initial);
    const write = writer(cache);
    write(initial);
    write(final);
    expect(cache.readQuery({ query })).toEqual({
      messages: [{ ...first, name: "Alice" }],
    });
  });

  test("writes embedded targets through the native writer", () => {
    const document = gql`
      query {
        message {
          id
          profile {
            text
            ... @defer {
              name
            }
          }
        }
      }
    `;
    const cache = new ForestRun();
    const write = writer(cache, document);
    write({
      data: {
        message: { ...first, profile: { __typename: "Profile", text: "old" } },
      },
      pending: [{ path: ["message", "profile"] }],
      hasNext: true,
    });
    write({
      ...final,
      incremental: [{ path: ["message", "profile"], data: { name: "Alice" } }],
    });
    expect(cache.readQuery({ query: document })).toMatchObject({
      message: { profile: { text: "old", name: "Alice" } },
    });
  });

  test("uses original aliases and arguments", () => {
    const document = gql`
      query {
        messages {
          id
          ... @defer {
            display: name(locale: "en")
          }
        }
      }
    `;
    const cache = new ForestRun();
    const write = writer(cache, document);
    write({
      ...initial,
      data: { messages: [{ __typename: "Message", id: "1" }] },
    });
    write({
      ...final,
      incremental: [{ path: ["messages", 0], data: { display: "Alice" } }],
    });
    expect(cache.readQuery({ query: document })).toEqual({
      messages: [{ __typename: "Message", id: "1", display: "Alice" }],
    });
  });

  test("does not resurrect evicted targets and releases terminal metadata", () => {
    const cache = new ForestRun();
    const write = writer(cache);
    write(initial);
    cache.evict({ id: "Message:1" });
    const before = cache.getStats();
    write(final);
    expect(cache.getStats()).toEqual(before);
    const next = writer(cache);
    next(initial);
    next({ hasNext: false });
    expect(cache["deferredWrites"].size).toBe(0);
  });
});
