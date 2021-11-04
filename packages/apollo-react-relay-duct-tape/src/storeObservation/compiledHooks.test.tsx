import React from "react";
import { ApolloProvider, QueryResult } from "@apollo/client";
import {
  act,
  create as createTestRenderer,
  ReactTestRenderer,
} from "react-test-renderer";
import { buildSchema } from "graphql";
import { graphql } from "@graphitation/graphql-js-tag";
import {
  ApolloMockClient,
  createMockClient,
} from "@graphitation/apollo-mock-client";
import * as MockPayloadGenerator from "@graphitation/graphql-js-operation-payload-generator";
import * as fs from "fs";
import * as path from "path";
import { print } from "graphql";

import {
  useCompiledFragment,
  useCompiledLazyLoadQuery,
  useCompiledRefetchableFragment,
} from "./compiledHooks";
import { typePolicies } from "./typePolicies";

/**
 * NOTE: These compiler artefacts are normally imported using the transform from the createImportDocumentsTransform.ts module
 */
import * as compiledHooks_Root_executionQuery_documents from "./__generated__/compiledHooks_Root_executionQuery.graphql";
import * as compiledHooks_ChildFragment_documents from "./__generated__/compiledHooks_ChildWatchNodeQuery.graphql";
// TODO: Emit this import from transform
import * as compiledHooks_RefetchableFragment_documents from "./__generated__/compiledHooks_RefetchableFragment_RefetchQuery.graphql";
import { compiledHooks_Root_executionQueryVariables } from "./__generated__/compiledHooks_Root_executionQuery.graphql";

const schema = buildSchema(
  fs.readFileSync(
    path.resolve(__dirname, "../__tests__/schema.graphql"),
    "utf8"
  )
);

const Child_fragment = graphql`
  fragment compiledHooks_ChildFragment on User {
    petName
  }
`;

const Refetchable_fragment = graphql`
  fragment compiledHooks_RefetchableFragment on User
  # @argumentDefinitions(avatarSize: { type: "Int!", defaultValue: 21 })
  @refetchable(queryName: "compiledHooks_RefetchableFragment_RefetchQuery") {
    avatarUrl(size: $avatarSize)
  }
`;

const Root_executionQueryDocument = graphql`
  query compiledHooks_Root_executionQuery($userId: ID!, $avatarSize: Int!) {
    user(id: $userId) {
      name
      ...compiledHooks_ChildFragment
      ...compiledHooks_RefetchableFragment
    }
  }
  ${Child_fragment}
  ${Refetchable_fragment}
`;

describe("compiledHooks", () => {
  let client: ApolloMockClient;
  let testRenderer: ReactTestRenderer;

  let lastUseFragmentResult: { id: number }[];
  let lastUseRefetchableFragmentResult: ReturnType<
    typeof useCompiledRefetchableFragment
  >[];
  let lastUseLazyLoadQueryResult: { data?: any; error?: Error } | null = null;

  const ChildComponent: React.FC<{ user: { id: any } }> = (props) => {
    const result = useCompiledFragment(
      compiledHooks_ChildFragment_documents as any,
      props.user
    );
    lastUseFragmentResult.push(result as { id: number });
    return null;
  };

  const ChildRefetchableComponent: React.FC<{ user: { id: any } }> = (
    props
  ) => {
    const result = useCompiledRefetchableFragment(
      compiledHooks_RefetchableFragment_documents as any,
      props.user
    );
    lastUseRefetchableFragmentResult.push(result);
    return null;
  };

  const RootComponent: React.FC<{ variables: {} }> = (props) => {
    const result = useCompiledLazyLoadQuery(
      compiledHooks_Root_executionQuery_documents as any,
      { variables: props.variables }
    );
    lastUseLazyLoadQueryResult = result;
    return result.data ? (
      <>
        <ChildComponent user={result.data.user} />
        <ChildRefetchableComponent user={result.data.user} />
      </>
    ) : null;
  };

  beforeEach(() => {
    lastUseFragmentResult = [];
    lastUseRefetchableFragmentResult = [];
    client = createMockClient(schema, {
      cache: {
        possibleTypes: {
          Node: ["User"],
        },
        typePolicies,
      },
    });
    act(() => {
      testRenderer = createTestRenderer(
        <ApolloProvider client={client}>
          <ErrorBoundary>
            <RootComponent variables={{ userId: 42, avatarSize: 21 }} />
          </ErrorBoundary>
        </ApolloProvider>
      );
    });
  });

  afterEach(() => {
    lastUseLazyLoadQueryResult = null;
  });

  describe(useCompiledLazyLoadQuery, () => {
    it("correctly returns loading state", async () => {
      expect(lastUseLazyLoadQueryResult).toEqual({
        data: undefined,
        error: undefined,
      });
      await act(() =>
        client.mock.resolveMostRecentOperation((operation) =>
          MockPayloadGenerator.generate(operation)
        )
      );
      expect(lastUseLazyLoadQueryResult).toEqual({
        data: expect.objectContaining({}),
        error: undefined,
      });
    });

    describe("once loaded", () => {
      beforeEach(async () => {
        await act(() =>
          client.mock.resolveMostRecentOperation((operation) =>
            MockPayloadGenerator.generate(operation, {
              User: () => ({ id: operation.request.variables.userId }),
            })
          )
        );
      });

      it("loads all data of the execution query into the store", () => {
        expect(client.cache.extract()["User:42"]).toMatchInlineSnapshot(`
          Object {
            "__typename": "User",
            "avatarUrl({\\"size\\":21})": "<mock-value-for-field-\\"avatarUrl\\">",
            "id": 42,
            "name": "<mock-value-for-field-\\"name\\">",
            "petName": "<mock-value-for-field-\\"petName\\">",
          }
        `);
      });

      it("only returns the fields selected in the watch query to the component", () => {
        expect(lastUseLazyLoadQueryResult!.data).toMatchInlineSnapshot(`
          Object {
            "user": Object {
              "__fragments": Object {
                "avatarSize": 21,
                "userId": 42,
              },
              "__typename": "User",
              "id": 42,
              "name": "<mock-value-for-field-\\"name\\">",
            },
          }
        `);
      });

      it("does not re-render when a field that was not selected in the watch query is updated in the store", async () => {
        const before = lastUseLazyLoadQueryResult!.data;
        await act(async () => {
          client.cache.modify({
            id: "User:42",
            fields: {
              petName: () => "Phoenix",
            },
          });
          return new Promise((resolve) => setTimeout(resolve, 0));
        });
        expect(lastUseLazyLoadQueryResult!.data).toBe(before);
      });

      it("does re-render when a field that was selected in the watch query is updated in the store", async () => {
        const before = lastUseLazyLoadQueryResult!.data;
        await act(async () => {
          client.cache.modify({
            id: "User:42",
            fields: {
              name: () => "Satya",
            },
          });
          return new Promise((resolve) => setTimeout(resolve, 0));
        });
        expect(lastUseLazyLoadQueryResult!.data).not.toBe(before);
        expect(lastUseLazyLoadQueryResult!.data).toMatchInlineSnapshot(`
          Object {
            "user": Object {
              "__fragments": Object {
                "avatarSize": 21,
                "userId": 42,
              },
              "__typename": "User",
              "id": 42,
              "name": "Satya",
            },
          }
        `);
      });

      it("fetches new data when variables change", async () => {
        act(() => {
          testRenderer.update(
            <ApolloProvider client={client}>
              <RootComponent variables={{ userId: 21, avatarSize: 21 }} />
            </ApolloProvider>
          );
        });
        await act(() =>
          client.mock.resolveMostRecentOperation((operation) =>
            MockPayloadGenerator.generate(operation, {
              User: () => ({ id: operation.request.variables.userId }),
            })
          )
        );
        expect(client.cache.extract()["User:21"]).toMatchInlineSnapshot(`
          Object {
            "__typename": "User",
            "avatarUrl({\\"size\\":21})": "<mock-value-for-field-\\"avatarUrl\\">",
            "id": 21,
            "name": "<mock-value-for-field-\\"name\\">",
            "petName": "<mock-value-for-field-\\"petName\\">",
          }
        `);
      });
    });
  });

  function itBehavesLikeFragment(
    returnedResults: () => { id: number }[],
    fragmentSpecificFieldSelection: string
  ) {
    beforeEach(async () => {
      await act(() =>
        client.mock.resolveMostRecentOperation((operation) => {
          const result = MockPayloadGenerator.generate(operation, {
            User: () => ({
              id: 42,
            }),
          });
          return result;
        })
      );
    });

    it("only returns the fields selected in the watch query to the component", () => {
      expect(returnedResults()[0]).toEqual({
        __fragments: {
          avatarSize: 21,
          userId: 42,
        },
        __typename: "User",
        [fragmentSpecificFieldSelection]: `<mock-value-for-field-"${fragmentSpecificFieldSelection}">`,
        id: 42,
      });
    });

    it("returns the same object when a field that was not selected in the watch query is updated in the store", async () => {
      const before = returnedResults()[0];
      await act(async () => {
        client.cache.modify({
          id: "User:42",
          fields: {
            name: () => "Satya",
          },
        });
        return new Promise((resolve) => setTimeout(resolve, 0));
      });
      // TODO: Check if this makes sense
      // expect(returnedResults().length).toBe(2);
      expect(returnedResults()[1]).toBe(before);
    });

    it("returns a new object when a field that was selected in the watch query is updated in the store", async () => {
      await act(async () => {
        client.cache.modify({
          id: "User:42",
          fields: {
            [fragmentSpecificFieldSelection]: () => "some new value",
          },
        });
        return new Promise((resolve) => setTimeout(resolve, 0));
      });
      expect(returnedResults().length).toBe(2);
      expect(returnedResults()[1]).toMatchObject({
        [fragmentSpecificFieldSelection]: "some new value",
      });
    });

    it("returns data synchronously", () => {
      expect(returnedResults().length).toBe(1);
    });

    it("fetches new data when variables change", async () => {
      act(() => {
        testRenderer.update(
          <ApolloProvider client={client}>
            <RootComponent variables={{ userId: 21 }} />
          </ApolloProvider>
        );
      });
      await act(() =>
        client.mock.resolveMostRecentOperation((operation) =>
          MockPayloadGenerator.generate(operation, {
            User: () => ({ id: operation.request.variables.userId }),
          })
        )
      );
      expect(returnedResults()[1].id).toBe(21);
    });
  }

  describe(useCompiledFragment, () => {
    itBehavesLikeFragment(() => lastUseFragmentResult, "petName");
  });

  describe(useCompiledRefetchableFragment, () => {
    itBehavesLikeFragment(
      () =>
        lastUseRefetchableFragmentResult.map(
          ([data, _refetch]) => data as { id: number }
        ),
      "avatarUrl"
    );

    it.todo(
      "supports variables with default values on either operations or with @argumentDefinitions"
    );

    describe("when refetching", () => {
      let onCompleted: jest.Mock;

      beforeEach(() => {
        const [_data, refetch] = lastUseRefetchableFragmentResult[0];
        onCompleted = jest.fn();
        refetch({ avatarSize: 42 }, { onCompleted });
      });

      describe("successfully", () => {
        beforeEach(async () => {
          await act(() => {
            client.mock.resolveMostRecentOperation((operation) =>
              MockPayloadGenerator.generate(operation, {
                Node: () => ({
                  id: 42,
                  avatarUrl: `avatarUrl-with-size-${operation.request.variables.avatarSize}`,
                }),
              })
            );
            return new Promise((resolve) => setTimeout(resolve, 0));
          });
        });

        it("returns a new object from the hook", () => {
          expect(lastUseRefetchableFragmentResult[1][0]).toMatchObject({
            __typename: "User",
            avatarUrl: "avatarUrl-with-size-42",
            id: 42,
          });
        });

        it("updates the fragment reference request variables for future requests", () => {
          expect(lastUseRefetchableFragmentResult[1][0]).toMatchObject({
            __fragments: {
              avatarSize: 42,
              userId: 42,
            },
          });
        });

        it("invokes the onComplete callback without error", () => {
          expect(onCompleted).toHaveBeenCalledWith(null);
        });
      });

      describe("and an error occurs", () => {
        const error = new Error("oh noes");

        beforeEach(async () => {
          await act(() => client.mock.rejectMostRecentOperation(error));
        });

        it("invokes the onComplete callback when an error occurs", () => {
          expect(onCompleted).toHaveBeenCalledWith(error);
        });

        it("does not update the fragment reference request variables for future requests", async () => {
          const [_data, refetch] = lastUseRefetchableFragmentResult[0];
          refetch({});
          expect(
            client.mock.getMostRecentOperation().request.variables.avatarSize
          ).toBe(21);
        });
      });
    });
  });
});

class ErrorBoundary extends React.Component {
  state = { hasError: false };

  static getDerivedStateFromError(error: Error) {
    return { hasError: true };
  }

  componentDidCatch(error: Error, errorInfo: any) {
    console.error({ error, errorInfo });
  }

  render() {
    if (this.state.hasError) {
      return <h1>Something went wrong.</h1>;
    }

    return this.props.children;
  }
}
