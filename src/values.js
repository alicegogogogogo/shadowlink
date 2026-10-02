import { ValidationError } from "./errors.js";

export function isPlainObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Structural equality; object key order is irrelevant, array order is not. */
export function deepEqual(left, right) {
  if (left === right) {
    return true;
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    return left.length === right.length && left.every((item, index) => deepEqual(item, right[index]));
  }
  if (isPlainObject(left) && isPlainObject(right)) {
    const keys = Object.keys(left);
    return keys.length === Object.keys(right).length && keys.every((key) => Object.hasOwn(right, key) && deepEqual(left[key], right[key]));
  }
  return false;
}

/**
 * Recursive merge used by shadow updates. A `null` value deletes the key at
 * any depth, nested objects merge, and every other value replaces the target.
 */
export function mergeValues(target, patch) {
  for (const [key, value] of Object.entries(patch)) {
    if (value === null) {
      delete target[key];
    } else if (isPlainObject(value) && isPlainObject(target[key])) {
      mergeValues(target[key], value);
    } else {
      target[key] = structuredClone(value);
    }
  }
  return target;
}

/**
 * Resolve a dotted path inside a document. The empty path is the document
 * itself; a numeric segment indexes an array. A missing segment resolves to
 * `undefined`.
 */
export function readPath(document, path) {
  if (path === "") {
    return document;
  }
  let current = document;
  for (const segment of path.split(".")) {
    if (Array.isArray(current)) {
      if (!/^\d+$/.test(segment)) {
        return undefined;
      }
      current = current[Number(segment)];
    } else if (isPlainObject(current)) {
      current = current[segment];
    } else {
      return undefined;
    }
    if (current === undefined) {
      return undefined;
    }
  }
  return current;
}

export function validatePath(path, label = "path") {
  if (typeof path !== "string" || path.length > 256) {
    throw new ValidationError(`${label} must be a string of at most 256 characters`);
  }
  if (path === "") {
    return path;
  }
  if (path.split(".").some((segment) => segment === "")) {
    throw new ValidationError(`${label} must not contain empty segments`);
  }
  return path;
}
