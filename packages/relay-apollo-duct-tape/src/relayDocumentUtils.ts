import Parser from "./vendor/relay-compiler/lib/core/RelayParser";
import CompilerContext from "./vendor/relay-compiler/lib/core/CompilerContext";
import { create as createSchema } from "./vendor/relay-compiler/lib/core/Schema";
import * as FlattenTransform from "./vendor/relay-compiler/lib/transforms/FlattenTransform";
import * as InlineFragmentsTransform from "./vendor/relay-compiler/lib/transforms/InlineFragmentsTransform";
import { generate as generateIRDocument } from "./vendor/relay-compiler/lib/codegen/RelayCodeGenerator";

import { Source, print as printGraphQLJS } from "graphql";
import hash from "@emotion/hash";

import type { DefinitionNode, DocumentNode } from "graphql";
import type { Schema } from "relay-compiler";
import type { Request } from "relay-compiler/lib/core/IR";

// TODO: Hash input document instead, which means memoization can skip
//       actually applying this transform.
export function transformDocument(
  schema: Schema,
  document: DocumentNode,
  addHash: boolean,
) {
  const nodes = Parser.transform(
    schema,
    document.definitions as DefinitionNode[],
  );
  let compilerContext = new CompilerContext(schema);
  for (const node of nodes) {
    compilerContext = compilerContext.add(node);
  }
  let operationCompilerContext = compilerContext.applyTransform(
    InlineFragmentsTransform.transform,
  );
  let fragmentCompilerContext = compilerContext.applyTransform(
    FlattenTransform.transformWithOptions({
      isForCodegen: true,
    } as any),
  );
  const res: any[] = [];
  operationCompilerContext.forEachDocument((node) => {
    if (node.kind === "Root") {
      const fragment = operationCompilerContext.getRoot(node.name);
      const name = fragment.name;
      const request: Request = {
        kind: "Request",
        fragment: {
          kind: "Fragment",
          name,
          argumentDefinitions: fragment.argumentDefinitions,
          directives: fragment.directives,
          loc: { kind: "Derived", source: node.loc },
          metadata: undefined,
          selections: fragment.selections as any,
          type: fragment.type,
        },
        id: undefined,
        loc: node.loc,
        metadata: node.metadata || {},
        name: fragment.name,
        root: node,
        text: "",
      };
      const generatedNode = generateIRDocument(schema, request);
      res.push(generatedNode);
    }
  });
  fragmentCompilerContext.forEachDocument((node) => {
    if (node.kind === "Fragment") {
      res.push(generateIRDocument(schema, node));
    }
  });
  const x = res[0];
  if (addHash) {
    x.hash = hash(JSON.stringify(x));
  }
  return x;
}

export function transformSchema(schema: DocumentNode): Schema {
  return createSchema(new Source(printGraphQLJS(schema)));
}
