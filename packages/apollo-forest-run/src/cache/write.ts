import type {
  DeferredPlaceholders,
  IndexedTree,
  UpdateTreeResult,
} from "../forest/types";
import type {
  CompositeListChunk,
  ObjectChunk,
  OperationResult,
  SourceObject,
} from "../values/types";
import { ValueKind } from "../values/types";
import type {
  CacheEnv,
  DataForest,
  DataTree,
  ModifyResult,
  OptimisticLayer,
  Store,
  Transaction,
  WriteResult,
  WriteOptions,
} from "./types";
import type { NodeKey, OperationDescriptor } from "../descriptor/types";
import { assert } from "../jsutils/assert";
import {
  isFragmentDocument,
  resolveResultDescriptor,
  resolveOperationDescriptor,
  ROOT_NODES,
  ROOT_TYPES,
} from "./descriptor";
import { applyMergePolicies } from "./policies";
import {
  getActiveForest,
  getEffectiveReadLayers,
  touchOperation,
} from "./store";
import { diffTree, GraphDifference } from "../diff/diffTree";
import {
  resolveAffectedOperations,
  updateAffectedTrees,
} from "../forest/updateForest";
import { indexTree } from "../forest/indexTree";
import { createParentLocator, markAsPartial, TraverseEnv } from "../values";
import { NodeDifferenceMap } from "../forest/updateTree";
import { getNodeChunks } from "./draftHelpers";
import { replaceTree } from "../forest/addTree";
import { invalidateReadResults } from "./invalidate";
import { IndexedForest } from "../forest/types";

export function write(
  env: CacheEnv,
  store: Store,
  activeTransaction: Transaction,
  options: WriteOptions,
  deferred?: DeferredPlaceholders,
): WriteResult | undefined {
  const { mergePolicies, objectKey, addTypename, keyMap } = env;
  const targetForest = getActiveForest(store, activeTransaction);

  let writeData =
    typeof options.result === "object" && options.result !== null
      ? options.result
      : {};
  const placeholder =
    options.defer?.path && deferred?.get(options.defer.path.join("."));
  if (options.defer?.path) {
    // Released or reset executions have no targets left to update.
    if (!deferred) return;
    assert(placeholder, "Deferred path does not identify an original target");
    if (getNodeChunks([targetForest], placeholder.key).next().done) return;
    const ownerData = placeholder.path.length
      ? embedInOwner(targetForest, placeholder, writeData)
      : writeData;
    if (!ownerData) return;
    writeData = ownerData;
    options = { ...options, dataId: placeholder.key };
  }
  const rootNodeKey = options.dataId ?? objectKey(writeData);
  assert(rootNodeKey !== false);

  // ApolloCompat (apollo allows writing fragments without proper id in the data)
  if (rootNodeKey !== undefined && options.dataId) {
    keyMap?.set(writeData, rootNodeKey);
  }
  const operationDescriptor = placeholder
    ? {
        ...placeholder.operation,
        rootNodeKey: placeholder.key,
        rootType: placeholder.type,
        possibleSelections: placeholder.selection,
        cache: false,
      }
    : resolveOperationDescriptor(
        env,
        store,
        options.query,
        options.variables,
        rootNodeKey,
      );
  touchOperation(env, store, operationDescriptor);
  if (!placeholder && !operationDescriptor.cache) {
    // Results of this operation are never stored, so nothing will ever evict its descriptor.
    // Remember it, so that the outermost transaction can release it on completion.
    // (the set only exists when `env.cleanupNonCacheableOperations` is enabled)
    activeTransaction.nonCacheableOperations?.add(operationDescriptor);
  }
  const operationResult: OperationResult = { data: writeData };

  if (
    !ROOT_TYPES.includes(operationDescriptor.rootType) &&
    rootNodeKey === undefined
  ) {
    throw new Error(`Could not identify object ${inspect(writeData)}`);
  }

  let existingResult = placeholder
    ? undefined
    : getExistingResult(env, store, targetForest, operationDescriptor);
  const existingData = existingResult?.result.data;

  // Safeguard: make sure previous state doesn't leak outside write operation
  assert(!existingResult?.prev);

  const pending = options.defer?.pending?.length ? deferred : undefined;
  if (writeData === existingData && existingResult && !pending) {
    return {
      options,
      incoming: existingResult,
      affected: [],
      difference: undefined,
      affectedNodes: new Set(),
      updateStats: [],
    };
  }

  if (!placeholder && !ROOT_NODES.includes(operationDescriptor.rootNodeKey)) {
    const typeName = resolveExtraRootNodeType(
      env,
      store,
      operationDescriptor,
      writeData,
    );
    if (addTypename && typeName && !writeData["__typename"]) {
      writeData["__typename"] = typeName;
    }
    targetForest.extraRootIds.set(
      operationDescriptor.rootNodeKey,
      typeName ?? "",
    );
    operationDescriptor.rootType = typeName ?? "";
  }

  const incomingResult = indexTree(
    env,
    operationDescriptor,
    operationResult,
    undefined,
    existingResult,
    !!pending,
  );
  if (pending) registerPlaceholders(incomingResult, options, pending);

  // ApolloCompat: necessary for fragment writes with custom ids
  if (options.dataId && incomingResult.rootNodeKey !== options.dataId) {
    const rootNode = incomingResult.nodes.get(incomingResult.rootNodeKey);
    assert(rootNode);
    incomingResult.nodes.set(options.dataId, rootNode);
    incomingResult.nodes.delete(incomingResult.rootNodeKey);
    incomingResult.rootNodeKey = options.dataId;
  }

  const modifiedIncomingResult = applyMergePolicies(
    env,
    getEffectiveReadLayers(store, targetForest, false),
    mergePolicies,
    incomingResult,
    options.overwrite ?? false,
  );

  const difference = diffTree(targetForest, modifiedIncomingResult, env);

  if (difference.errors.length) {
    processDiffErrors(targetForest, modifiedIncomingResult, difference);
  }

  if (
    existingResult &&
    existingResult.grown &&
    existingResult.incompleteChunks.size > 0
  ) {
    // Remove incomplete placeholder tree (saves unnecessary update)
    targetForest.trees.delete(operationDescriptor.id);
    existingResult = undefined;
  }

  // This function returns exhaustive list of affected operations. It may contain false-positives,
  // because operationsWithNodes also reflects nodes from optimistic updates and read policy results
  // (which may not exist in the main forest trees)
  const affectedOperations = resolveAffectedOperations(
    targetForest,
    difference,
  );

  const chunkProvider = (key: NodeKey) =>
    getNodeChunks(getEffectiveReadLayers(store, targetForest, false), key);

  const allUpdates = updateAffectedTrees(
    env,
    targetForest,
    affectedOperations,
    chunkProvider,
    modifiedIncomingResult,
  );

  if (!existingResult && shouldCache(targetForest, operationDescriptor)) {
    affectedOperations.set(operationDescriptor, difference.nodeDifference);
    // Note: even with existingResult === undefined the tree for this operation may still exist in the cache
    //   (when existingResult is resolved with a different key descriptor due to key variables)
    // TODO: replace with addTree and add a proper check for keyVariables
    replaceTree(env, targetForest, modifiedIncomingResult);
  }

  appendAffectedOperationsFromOtherLayers(
    env,
    store,
    affectedOperations,
    targetForest,
    modifiedIncomingResult,
  );

  invalidateReadResults(
    env,
    store,
    targetForest,
    difference,
    affectedOperations,
    modifiedIncomingResult,
  );
  incomingResult.prev = null;
  modifiedIncomingResult.prev = null;

  return {
    options,
    incoming: modifiedIncomingResult,
    affected: affectedOperations.keys(),
    difference,
    affectedNodes: aggregateAllAffectedNodes(difference, allUpdates),
    updateStats: allUpdates.map((update) => update.stats ?? null),
  };
}

// Binds each pending path to its nearest keyed object (owner) and the path below it.
function registerPlaceholders(
  tree: IndexedTree,
  options: WriteOptions,
  placeholders: DeferredPlaceholders,
) {
  for (const { path } of options.defer?.pending ?? EMPTY_ARRAY) {
    let source = tree.result.data as Record<string, unknown> | undefined;
    let owner;
    let ownerPath: (string | number)[] = [];
    for (let depth = 0; source && typeof source === "object"; depth++) {
      const chunk = tree.dataMap.get(source as SourceObject)?.value as
        | ObjectChunk
        | CompositeListChunk
        | undefined;
      if (chunk?.kind === ValueKind.Object && chunk.key !== false) {
        const type = chunk.type || (!depth && tree.operation.rootType);
        assert(type, "Deferred owners must include __typename");
        owner = { key: chunk.key, type, selection: chunk.possibleSelections };
        ownerPath = [];
      }
      if (depth === path.length) {
        assert(owner);
        placeholders.set(path.join("."), {
          ...owner,
          operation: tree.operation,
          path: ownerPath,
        });
        break;
      }
      ownerPath.push(path[depth]);
      source = source[path[depth]] as Record<string, unknown> | undefined;
    }
  }
}

// Writes an ID-less deferred target as a sparse nested write to its keyed owner. Like Apollo,
// positions below the owner are resolved against its current data in the original operation.
function embedInOwner(
  forest: IndexedForest,
  placeholder: NonNullable<ReturnType<DeferredPlaceholders["get"]>>,
  data: Record<string, unknown>,
): Record<string, unknown> | undefined {
  const { path } = placeholder;
  const embed = (source: Record<string, unknown>, depth: number): unknown => {
    if (depth === path.length) {
      return source.__typename === undefined
        ? data
        : { ...data, __typename: source.__typename };
    }
    const part = path[depth];
    const child = source[part];
    const value =
      child && typeof child === "object"
        ? embed(child as Record<string, unknown>, depth + 1)
        : undefined;
    if (value === undefined) return undefined;
    if (Array.isArray(source)) {
      const list = source.slice();
      list[part as number] = value;
      return list;
    }
    return source.__typename === undefined
      ? { [part]: value }
      : { __typename: source.__typename, [part]: value };
  };
  const tree = forest.trees.get(placeholder.operation.id);
  for (const owner of tree?.nodes.get(placeholder.key) ?? EMPTY_ARRAY) {
    const result = embed(owner.data as Record<string, unknown>, 0);
    if (result) return result as Record<string, unknown>;
  }
  return undefined;
}

function appendAffectedOperationsFromOtherLayers(
  env: CacheEnv,
  store: Store,
  affectedForestOperationsMutable: Map<OperationDescriptor, NodeDifferenceMap>,
  targetForest: DataForest | OptimisticLayer,
  incomingResult: IndexedTree,
) {
  // Optimistic reads go through all existing layers
  //  And those layers may be affected by incoming results too, so we actually need to diff all other layers too
  //  TODO: just write to all effective layers?
  for (const layer of getEffectiveReadLayers(store, targetForest, true)) {
    if (layer === targetForest) {
      continue;
    }
    resolveAffectedOperations(
      layer,
      diffTree(layer, incomingResult, env),
      affectedForestOperationsMutable,
    );
  }
}

function processDiffErrors(
  forest: DataForest | OptimisticLayer,
  model: IndexedTree,
  difference: GraphDifference,
) {
  const pathEnv: TraverseEnv = {
    findParent: (chunk) => {
      const tree = forest.trees.get(chunk.operation.id);
      const parentInfo = tree?.dataMap.get(chunk.data);
      assert(parentInfo);
      return parentInfo;
    },
  };

  for (const diffError of difference.errors) {
    if (diffError.kind === "MissingFields") {
      for (const baseChunkError of diffError.base ?? EMPTY_ARRAY) {
        // Missing chunks
        const chunk = baseChunkError.chunk;
        chunk.missingFields ??= new Set();
        for (const field of baseChunkError.missingFields) {
          chunk.missingFields.add(field);
        }
        const tree = forest.trees.get(chunk.operation.id);
        if (tree) {
          tree.incompleteChunks.add(chunk);
        }
        const parentInfo = pathEnv.findParent(chunk);
        markAsPartial(pathEnv, parentInfo);
      }

      pathEnv.findParent = createParentLocator(model.dataMap);
      for (const modelChunkError of diffError.model ?? EMPTY_ARRAY) {
        // Missing chunks
        const chunk = modelChunkError.chunk;
        chunk.missingFields ??= new Set();
        for (const field of modelChunkError.missingFields) {
          chunk.missingFields.add(field);
        }
        const parentInfo = pathEnv.findParent(chunk);
        markAsPartial(pathEnv, parentInfo);
        model.incompleteChunks.add(chunk);
      }
    }
  }
}

function getExistingResult(
  env: CacheEnv,
  store: Store,
  targetForest: IndexedForest,
  operation: OperationDescriptor,
): DataTree | undefined {
  const op = resolveResultDescriptor(env, store, operation);
  return targetForest.trees.get(op.id);
}

function shouldCache(
  targetForest: DataForest | OptimisticLayer,
  operation: OperationDescriptor,
) {
  // Always cache results for optimistic layers (even if operation is not cacheable, e.g. it is a mutation)
  if (targetForest.layerTag !== null) {
    return true;
  }
  return operation.cache;
}

function aggregateAllAffectedNodes(
  difference: GraphDifference,
  updates: UpdateTreeResult[],
): Set<NodeKey> {
  const accumulator = new Set<NodeKey>([
    ...difference.newNodes,
    ...difference.nodeDifference.keys(),
  ]);
  for (const { affectedNodes } of updates) {
    for (const nodeKey of affectedNodes) {
      accumulator.add(nodeKey);
    }
  }
  return accumulator;
}

function resolveExtraRootNodeType(
  env: CacheEnv,
  store: Store,
  operationDescriptor: OperationDescriptor,
  data: Record<string, unknown> & { __typename?: string },
): string | undefined {
  if (data["__typename"]) {
    return data["__typename"];
  }
  // Try fragment condition (fragments on abstract types are ignored)
  if (isFragmentDocument(operationDescriptor.document)) {
    const [fragmentDef] = operationDescriptor.fragmentMap.values();
    const typeName = fragmentDef?.typeCondition.name.value;
    if (!env.possibleTypes?.[typeName]) {
      return typeName;
    }
  }
  // Finally, try from store
  const [chunk] = getNodeChunks(
    [store.dataForest, ...store.optimisticLayers],
    operationDescriptor.rootNodeKey,
  );
  if (chunk?.type) {
    return chunk.type;
  }
  return undefined;
}

const inspect = JSON.stringify.bind(JSON);
const EMPTY_ARRAY = Object.freeze([]);

export function isWrite(op: WriteResult | ModifyResult): op is WriteResult {
  return "incoming" in op; // write has "incoming" tree
}
