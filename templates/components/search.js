window.LogApp = window.LogApp || {};

LogApp.buildSearchParser = () => {
  const getFieldValue = (event, path) => {
    if (!event || !path) return null;
    const parts = String(path)
      .replace(/^\.+/, "")
      .split(".");
    let current = event;
    for (const part of parts) {
      if (current == null || typeof current !== "object") return null;
      current = current[part];
    }
    return current;
  };

  const getFieldValues = (event, path) => {
    if (!event || !path) return [];
    if (path === "$") return [];
    if (path === "$.*") {
      return collectAnyValues(event);
    }
    if (path.startsWith("$.")) {
      const subPath = path.slice(2);
      if (!subPath) return [];
      return collectDeepValues(event, subPath.split("."));
    }
    const deepIndex = path.indexOf("$.");
    if (deepIndex === -1) {
      return [getFieldValue(event, path)];
    }
    const basePath = path.slice(0, deepIndex);
    const subPath = path.slice(deepIndex + 2);
    const baseValue = basePath ? getFieldValue(event, basePath) : event;
    if (baseValue == null) return [];
    if (!subPath || subPath === "*") return collectAnyValues(baseValue);
    if (subPath.endsWith(".*")) {
      const trimmed = subPath.slice(0, -2);
      const targets = collectDeepValues(baseValue, trimmed.split("."));
      return targets.flatMap((value) => (value == null ? [] : collectAnyValues(value)));
    }
    return collectDeepValues(baseValue, subPath.split("."));
  };

  const toComparable = (value) => {
    if (value == null) return "";
    if (typeof value === "string") return value;
    if (typeof value === "number" || typeof value === "boolean") return String(value);
    return JSON.stringify(value);
  };

  const globToRegex = (pattern) => {
    const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&");
    const regex = "^" + escaped.replace(/\*/g, ".*") + "$";
    return new RegExp(regex, "i");
  };

  const tokenizeQuery = (input) => {
    const tokens = [];
    const ts = makeTokenStream(input);
    while (true) {
      const token = ts.next();
      tokens.push(token);
      if (token.type === "EOF") break;
    }
    return tokens;
  };

  const parseQuery = (input) => {
    const ts = makeTokenStream(input || "");
    if (ts.peek().type === "EOF") return { type: "EMPTY" };
    const ast = parseExpression(ts, 0);
    ts.expect("EOF", "Unexpected extra input");
    return ast || { type: "EMPTY" };
  };

  const matchFieldTerm = (event, field, term) => {
    if (field === "$") {
      return matchKeyNameTerm(event, term);
    }
    const cleaned = term.startsWith('"') && term.endsWith('"') ? term.slice(1, -1) : term;

    const applyMatch = (candidate) => {
      if (candidate == null) return false;
      const text = toComparable(candidate);
      if (term.includes("*")) {
        return globToRegex(cleaned).test(text);
      }
      if (field === "name") {
        return globToRegex(cleaned + "*").test(text);
      }
      return text.toLowerCase() === cleaned.toLowerCase();
    };

    const values = getFieldValues(event, field);
    for (const value of values) {
      if (Array.isArray(value)) {
        if (value.some((item) => applyMatch(item))) return true;
        continue;
      }
      if (typeof value === "boolean") {
        if (cleaned.toLowerCase() === "true" && value === true) return true;
        if (cleaned.toLowerCase() === "false" && value === false) return true;
        continue;
      }
      if (typeof value === "number") {
        if (String(value) === cleaned) return true;
        continue;
      }
      if (applyMatch(value)) return true;
    }
    return false;
  };

  const matchBareTerm = (event, term) => {
    if (!term) return true;
    const nameValue = toComparable(event?.name || "");
    if (globToRegex(`${term}*`).test(nameValue)) return true;
    const entries = Object.entries(event || {});
    for (const [key, value] of entries) {
      if (key === "data") continue;
      if (Array.isArray(value)) {
        if (value.some((item) => toComparable(item).toLowerCase() === term.toLowerCase())) {
          return true;
        }
        continue;
      }
      const text = toComparable(value);
      if (text && text.toLowerCase() === term.toLowerCase()) return true;
    }
    return false;
  };

  const filterObjects = (objects, ast, options = {}) => {
    const { fieldHandlers = {} } = options;
    const intervalField = normalizeFieldKey(options.intervalField || "norm_time");
    const allIndexes = new Set(objects.map((_, index) => index));
    const evalCache = new WeakMap();
    const refValueCache = new Map();
    const boundaryCache = new Map();
    const intervalCache = new Map();
    const resultIndexes = evaluate(ast, objects);
    return Array.from(resultIndexes)
      .sort((a, b) => a - b)
      .map((index) => objects[index]);

    function evaluate(node, list) {
      if (!node) return new Set(allIndexes);
      if (evalCache.has(node)) return new Set(evalCache.get(node));
      let result;
      switch (node.type) {
        case "EMPTY":
          result = new Set(allIndexes);
          break;
        case "AND":
          result = node.terms.reduce(
            (acc, term) => intersectSets(acc, evaluate(term, list)),
            new Set(allIndexes)
          );
          break;
        case "OR":
          result = node.terms.reduce((acc, term) => unionSets(acc, evaluate(term, list)), new Set());
          break;
        case "NOT":
          result = subtractSets(new Set(allIndexes), evaluate(node.term, list));
          break;
        case "TEXT":
          result = matchIndexes(list, (obj) => matchBareTerm(obj, node.value));
          break;
        case "FILTER":
          result = matchIndexes(list, (obj) => evalFilter(node, obj, list));
          break;
        case "METHOD":
          result = evalMethod(node, list);
          break;
        case "RANGE":
          result = matchIndexes(list, (obj) => matchInOperator(obj, intervalField, node.value, list));
          break;
        default:
          result = new Set(allIndexes);
          break;
      }
      evalCache.set(node, new Set(result));
      return new Set(result);
    }

    function evalMethod(node, list) {
      const matches = Array.from(evaluate(node.term, list)).sort((a, b) => a - b);
      if (!matches.length) return new Set();
      if (node.name === "first") return new Set([matches[0]]);
      if (node.name === "last") return new Set([matches[matches.length - 1]]);
      return new Set(matches);
    }

    function evalFilter(node, obj, list) {
      const key = normalizeFieldKey(node.key);

      if (fieldHandlers[key]) {
        return fieldHandlers[key](obj, node.value, null, node.op);
      }

      return defaultFieldEval(key, node.value, obj, node.op, list);
    }

    function defaultFieldEval(key, valueNode, obj, op, list) {
      if (!valueNode) return false;
      if (key === "$" && op && op !== ":" && op !== "~") return false;
      if (op === "IN") return matchInOperator(obj, key, valueNode, list);
      if (valueNode.type === "REF") {
        return compareFieldToReference(obj, key, valueNode, op, list);
      }
      if (valueNode.type !== "TEXT" || valueNode.value == null) return false;
      if (!op || op === ":") {
        return matchFieldTerm(obj, key, valueNode.value);
      }
      if (op === "~") {
        return containsField(obj, key, valueNode.value);
      }
      if (![">", ">=", "<", "<="].includes(op)) return false;
      return compareField(obj, key, valueNode.value, op);
    }

    function matchInOperator(obj, key, valueNode, list) {
      const intervals = resolveIntervals(valueNode, key, list);
      if (!intervals.length) return false;
      const fieldValues = getFieldValues(obj, key);
      for (const fieldValue of fieldValues) {
        if (Array.isArray(fieldValue)) {
          if (fieldValue.some((item) => intervals.some((interval) => isWithinInterval(item, interval)))) {
            return true;
          }
          continue;
        }
        if (intervals.some((interval) => isWithinInterval(fieldValue, interval))) return true;
      }
      return false;
    }

    function resolveIntervals(node, key, list) {
      if (!node) return [];
      const cacheKey = `${key}:${serializeNode(node)}`;
      if (intervalCache.has(cacheKey)) return intervalCache.get(cacheKey);

      let intervals = [];
      if (node.type === "INTERVAL") {
        const start = resolveBoundaryValue(node.start, key, list, "first");
        const end = resolveBoundaryValue(node.end, key, list, "last");
        intervals = buildNormalizedIntervalList(start, end);
      } else if (node.type === "DURATION") {
        intervals = resolveDurationIntervals(node, key, list);
      }

      intervalCache.set(cacheKey, intervals);
      return intervals;
    }

    function resolveDurationIntervals(node, key, list) {
      const startMatches = Array.from(evaluate(node.start, list)).sort((a, b) => a - b);
      const endMatches = Array.from(evaluate(node.end, list)).sort((a, b) => a - b);
      const intervals = [];
      let startCursor = 0;
      let endCursor = 0;

      while (startCursor < startMatches.length && endCursor < endMatches.length) {
        const startIndex = startMatches[startCursor];
        while (endCursor < endMatches.length && endMatches[endCursor] <= startIndex) {
          endCursor += 1;
        }
        if (endCursor >= endMatches.length) break;

        const endIndex = endMatches[endCursor];
        const startValue = getBoundaryFieldValue(list[startIndex], key);
        const endValue = getBoundaryFieldValue(list[endIndex], key);
        intervals.push(...buildNormalizedIntervalList(startValue, endValue));

        startCursor += 1;
        endCursor += 1;
        while (startCursor < startMatches.length && startMatches[startCursor] <= endIndex) {
          startCursor += 1;
        }
      }

      return intervals;
    }

    function getBoundaryFieldValue(obj, key) {
      const values = getFieldValues(obj, key);
      if (!values.length) return null;
      const first = values[0];
      if (Array.isArray(first)) return first[0] ?? null;
      return first;
    }

    function buildNormalizedIntervalList(start, end) {
      const lower = Number(start);
      const upper = Number(end);
      if (!Number.isFinite(lower) || !Number.isFinite(upper)) return [];
      return [{ min: Math.min(lower, upper), max: Math.max(lower, upper) }];
    }

    function resolveBoundaryValue(node, key, list, selectionMode) {
      const cacheKey = `${selectionMode}:${key}:${serializeNode(node)}`;
      if (boundaryCache.has(cacheKey)) return boundaryCache.get(cacheKey);
      let resolved = null;
      if (!node) return null;
      if (node.type === "TEXT") resolved = node.value;
      else if (node.type === "REF") {
        const referenceKey = normalizeFieldKey(node.field || key);
        const values = resolveReferenceValues(node, referenceKey, list);
        resolved = selectionMode === "last" ? values[values.length - 1] : values[0];
      } else {
        const matches = Array.from(evaluate(node, list)).sort((a, b) => a - b);
        if (!matches.length) {
          boundaryCache.set(cacheKey, null);
          return null;
        }
        const pickedIndex = selectionMode === "last" ? matches[matches.length - 1] : matches[0];
        const picked = list[pickedIndex];
        const values = getFieldValues(picked, key);
        resolved = Array.isArray(values[0]) ? values[0][0] : values[0];
      }
      boundaryCache.set(cacheKey, resolved);
      return resolved;
    }

    function isWithinInterval(value, interval) {
      const num = Number(value);
      return Number.isFinite(num) && num >= interval.min && num <= interval.max;
    }

    function compareField(obj, key, rawValue, op) {
      if (key === "$") return false;
      const target = Number(rawValue);
      if (!Number.isFinite(target)) return false;
      const compareOne = (value) => {
        const num = Number(value);
        if (!Number.isFinite(num)) return false;
        switch (op) {
          case ">":
            return num > target;
          case ">=":
            return num >= target;
          case "<":
            return num < target;
          case "<=":
            return num <= target;
          default:
            return false;
        }
      };
      const values = getFieldValues(obj, key);
      for (const fieldValue of values) {
        if (Array.isArray(fieldValue)) {
          if (fieldValue.some((item) => compareOne(item))) return true;
          continue;
        }
        if (compareOne(fieldValue)) return true;
      }
      return false;
    }

    function compareFieldToReference(obj, key, refNode, op, list) {
      const referenceKey = normalizeFieldKey(refNode.field || key);
      const referenceValues = resolveReferenceValues(refNode, referenceKey, list);
      if (!referenceValues.length) return false;
      if (!op || op === ":") {
        return compareFieldAgainstValues(obj, key, referenceValues, equalsComparable);
      }
      if (op === "~") {
        return compareFieldAgainstValues(obj, key, referenceValues, containsComparable);
      }
      if (![">", ">=", "<", "<="].includes(op)) return false;
      return compareFieldAgainstValues(obj, key, referenceValues, (left, right) =>
        compareNumeric(left, right, op)
      );
    }

    function resolveReferenceValues(refNode, key, list) {
      const cacheKey = `${key}:${serializeNode(refNode.term)}`;
      if (refValueCache.has(cacheKey)) return refValueCache.get(cacheKey);
      const matches = Array.from(evaluate(refNode.term, list))
        .sort((a, b) => a - b)
        .map((index) => list[index]);
      const values = [];
      matches.forEach((item) => {
        getFieldValues(item, key).forEach((value) => {
          if (Array.isArray(value)) {
            value.forEach((entry) => values.push(entry));
            return;
          }
          values.push(value);
        });
      });
      refValueCache.set(cacheKey, values);
      return values;
    }

    function compareFieldAgainstValues(obj, key, referenceValues, compareFn) {
      const fieldValues = getFieldValues(obj, key);
      for (const fieldValue of fieldValues) {
        if (Array.isArray(fieldValue)) {
          for (const item of fieldValue) {
            if (referenceValues.some((ref) => compareFn(item, ref))) return true;
          }
          continue;
        }
        if (referenceValues.some((ref) => compareFn(fieldValue, ref))) return true;
      }
      return false;
    }

    function equalsComparable(left, right) {
      if (left == null || right == null) return false;
      return String(left).toLowerCase() === String(right).toLowerCase();
    }

    function containsComparable(left, right) {
      if (left == null || right == null) return false;
      return String(left).toLowerCase().includes(String(right).toLowerCase());
    }

    function compareNumeric(left, right, op) {
      const leftNum = Number(left);
      const rightNum = Number(right);
      if (!Number.isFinite(leftNum) || !Number.isFinite(rightNum)) return false;
      switch (op) {
        case ">":
          return leftNum > rightNum;
        case ">=":
          return leftNum >= rightNum;
        case "<":
          return leftNum < rightNum;
        case "<=":
          return leftNum <= rightNum;
        default:
          return false;
      }
    }

    function containsField(obj, key, rawValue) {
      if (key === "$") return matchKeyNameContains(obj, rawValue);
      const needle = String(rawValue).toLowerCase();
      const contains = (value) => {
        if (value == null) return false;
        return String(value).toLowerCase().includes(needle);
      };
      const values = getFieldValues(obj, key);
      for (const fieldValue of values) {
        if (Array.isArray(fieldValue)) {
          if (fieldValue.some((item) => contains(item))) return true;
          continue;
        }
        if (contains(fieldValue)) return true;
      }
      return false;
    }

    function matchIndexes(list, predicate) {
      const matches = new Set();
      list.forEach((obj, index) => {
        if (predicate(obj)) matches.add(index);
      });
      return matches;
    }

    function intersectSets(left, right) {
      const out = new Set();
      left.forEach((value) => {
        if (right.has(value)) out.add(value);
      });
      return out;
    }

    function unionSets(left, right) {
      const out = new Set(left);
      right.forEach((value) => out.add(value));
      return out;
    }

    function subtractSets(left, right) {
      const out = new Set(left);
      right.forEach((value) => out.delete(value));
      return out;
    }

    function normalizeFieldKey(key) {
      const normalized = String(key || "").replace(/^\.+/, "").toLowerCase();
      if (normalized === "time") return "norm_time";
      return normalized;
    }

    function serializeNode(node) {
      if (!node) return "null";
      return JSON.stringify(node);
    }
  };

  function collectDeepValues(root, pathParts) {
    const results = [];
    const seen = new Set();

    const walk = (node) => {
      if (node == null || typeof node !== "object") return;
      if (seen.has(node)) return;
      seen.add(node);

      const value = getFieldValue(node, pathParts.join("."));
      if (value != null) results.push(value);

      if (Array.isArray(node)) {
        node.forEach((item) => walk(item));
        return;
      }
      Object.values(node).forEach((child) => walk(child));
    };

    walk(root);
    return results;
  }

  function collectAnyValues(root) {
    const results = [];
    const seen = new Set();

    const walk = (node) => {
      if (node == null || typeof node !== "object") return;
      if (seen.has(node)) return;
      seen.add(node);

      if (Array.isArray(node)) {
        node.forEach((item) => walk(item));
        return;
      }
      Object.values(node).forEach((value) => {
        results.push(value);
        walk(value);
      });
    };

    walk(root);
    return results;
  }

  function matchKeyNameTerm(root, term) {
    const cleaned = term.startsWith('"') && term.endsWith('"') ? term.slice(1, -1) : term;
    const isWildcard = cleaned.includes("*");
    const matcher = isWildcard ? LogApp.globToRegex(cleaned) : null;
    return collectKeyNames(root).some((key) => {
      if (isWildcard) return matcher.test(key);
      return key.toLowerCase() === cleaned.toLowerCase();
    });
  }

  function matchKeyNameContains(root, term) {
    const needle = String(term).toLowerCase();
    return collectKeyNames(root).some((key) => key.toLowerCase().includes(needle));
  }

  function collectKeyNames(root) {
    const results = [];
    const seen = new Set();

    const walk = (node) => {
      if (node == null || typeof node !== "object") return;
      if (seen.has(node)) return;
      seen.add(node);

      if (Array.isArray(node)) {
        node.forEach((item) => walk(item));
        return;
      }
      Object.entries(node).forEach(([key, value]) => {
        results.push(String(key));
        walk(value);
      });
    };

    walk(root);
    return results;
  }

  const makePredicate = (query, options = {}) => {
    const ast = parseQuery(query);
    return (event) => filterObjects([event], ast, options).length === 1;
  };

  const filterQueryObjects = (objects, query, options = {}) => {
    const ast = parseQuery(query);
    const globalOptions =
      typeof window !== "undefined" && window.LogApp?.searchOptions ? window.LogApp.searchOptions : {};
    return filterObjects(objects, ast, { ...globalOptions, ...options });
  };

  const getQueryPredicate = (() => {
    const cache = new Map();
    return (query) => {
      const key = query || "";
      if (cache.has(key)) return cache.get(key);
      const predicate = makePredicate(key);
      cache.set(key, predicate);
      return predicate;
    };
  })();

  function makeTokenStream(input) {
    let i = 0;
    let buffered = null;

    const error = (msg, tok) => {
      const t = tok || buffered || { start: i };
      throw new SyntaxError(`${msg} at ${t.start}`);
    };

    const peek = () => {
      if (!buffered) buffered = readToken();
      return buffered;
    };

    const next = () => {
      const t = peek();
      buffered = null;
      return t;
    };

    const match = (type) => {
      if (peek().type === type) {
        next();
        return true;
      }
      return false;
    };

    const expect = (type, msg) => {
      const t = peek();
      if (t.type !== type) error(msg || `Expected ${type} but got ${t.type}`, t);
      return next();
    };

    const startsExpression = (type) =>
    type === "LPAREN" ||
    type === "WORD" ||
    type === "PHRASE" ||
    type === "AND" ||
    type === "NOT" ||
    type === "MINUS" ||
    type === "FIELD";

    const skipWhitespace = () => {
      while (i < input.length && isWhitespace(input[i])) i++;
    };

    const readToken = () => {
      skipWhitespace();
      const start = i;

      if (i >= input.length) return { type: "EOF", start: i, end: i };

      const ch = input[i];
      const prev = i > 0 ? input[i - 1] : "";

      if (ch === "(") return (i++, { type: "LPAREN", start, end: i });
      if (ch === ")") return (i++, { type: "RPAREN", start, end: i });
      if (ch === ",") return (i++, { type: "COMMA", start, end: i });
      if (ch === "@") return (i++, { type: "AT", start, end: i });
      if (ch === "|") return (i++, { type: "OR", start, end: i });
      if (ch === "~") return (i++, { type: "CONTAINS", op: "~", start, end: i });
      if (ch === ">" || ch === "<") {
        let op = ch;
        i += 1;
        if (input[i] === "=") {
          op += "=";
          i += 1;
        }
        return { type: "COMP", op, start, end: i };
      }
      if (ch === "-" && (i === 0 || isWhitespace(prev) || prev === "(")) {
        return (i++, { type: "MINUS", start, end: i });
      }

      if (ch === '"') {
        const value = readPhrase();
        return { type: "PHRASE", value, start, end: i };
      }

      const wordStart = i;
      const word = readWord();
      const upper = word.toUpperCase();

      if (i < input.length) {
        if (input[i] === ":") {
          i++;
          return { type: "FIELD", key: word, op: ":", start: wordStart, end: i };
        }
        if (input[i] === "~") {
          i++;
          return { type: "FIELD", key: word, op: "~", start: wordStart, end: i };
        }
        if (input[i] === ">" || input[i] === "<") {
          let op = input[i];
          i += 1;
          if (input[i] === "=") {
            op += "=";
            i += 1;
          }
          return { type: "FIELD", key: word, op, start: wordStart, end: i };
        }
      }

    if (upper === "OR") return { type: "OR", start: wordStart, end: i };
    if (upper === "AND") return { type: "AND", start: wordStart, end: i };
    if (upper === "NOT") return { type: "NOT", start: wordStart, end: i };
    if (upper === "IN") return { type: "IN", start: wordStart, end: i };
      return { type: "WORD", value: word, start: wordStart, end: i };
    };

    const isWhitespace = (c) => c === " " || c === "\t" || c === "\n" || c === "\r";

    const isWordChar = (c) =>
      c !== undefined &&
      !isWhitespace(c) &&
      c !== "(" &&
      c !== ")" &&
      c !== "," &&
      c !== ":" &&
      c !== '"' &&
      c !== "|" &&
      c !== "~" &&
      c !== ">" &&
      c !== "<";

    const readWord = () => {
      const start = i;
      while (i < input.length && isWordChar(input[i])) i++;
      if (i === start) error("Expected word");
      return input.slice(start, i);
    };

    const readPhrase = () => {
      i++;
      let out = "";
      while (i < input.length) {
        const ch = input[i];

        if (ch === "\\") {
          const nextChar = input[i + 1];
          if (nextChar === undefined) error("Unterminated escape in string");
          out += unescapeChar(nextChar);
          i += 2;
          continue;
        }
        if (ch === '"') {
          i++;
          return out;
        }
        out += ch;
        i++;
      }
      error("Unterminated quote");
    };

    const unescapeChar = (c) => {
      if (c === "n") return "\n";
      if (c === "t") return "\t";
      if (c === "r") return "\r";
      return c;
    };

    return { peek, next, match, expect, error, startsExpression };
  }

  function parseExpression(ts, minBp) {
    let left = parsePrimary(ts);

    while (true) {
      const next = ts.peek();
      let op = null;
      let lbp = 0;

    if (next && next.type === "OR") {
      op = "OR";
      lbp = 1;
    } else if (next && next.type === "AND") {
      op = "AND";
      lbp = 2;
    } else if (next && ts.startsExpression(next.type)) {
      op = "AND";
      lbp = 2;
    } else {
      break;
    }

      if (lbp < minBp) break;
    if (op === "OR" || op === "AND") ts.next();

      const right = parseExpression(ts, lbp + 1);
      if (op === "AND") {
        left = makeAnd([left, right]);
      } else {
        left = makeOr([left, right]);
      }
    }

    return left;
  }

  function parsePrimary(ts) {
    const token = ts.next();
    if (!token) return { type: "EMPTY" };
    if (token.type === "EOF") return { type: "EMPTY" };

    if (token.type === "WORD") {
      if (ts.peek().type === "IN") {
        const inToken = ts.next();
        const value = parseInValue(ts);
        return { type: "FILTER", key: token.value, op: inToken.type, value };
      }
      if (ts.peek().type === "COMP" || ts.peek().type === "CONTAINS") {
        const comp = ts.next();
        const value = parsePrimaryValue(ts);
        return { type: "FILTER", key: token.value, op: comp.op, value };
      }
      if (ts.peek().type === "LPAREN") {
        const methodName = token.value.toLowerCase();
        if (methodName === "first" || methodName === "last") {
          ts.next();
          const expr = parseExpression(ts, 0);
          ts.expect("RPAREN", "Expected ')'");
          return { type: "METHOD", name: methodName, term: expr };
        }
        if (methodName === "interval" || methodName === "duration") {
          const value =
            methodName === "interval"
              ? parseIntervalValueBody(ts)
              : parseDurationValueBody(ts);
          return { type: "RANGE", value };
        }
      }
      return { type: "TEXT", value: token.value, kind: "word" };
    }
    if (token.type === "PHRASE") {
      return { type: "TEXT", value: token.value, kind: "phrase" };
    }
    if (token.type === "MINUS" || token.type === "NOT") {
      return { type: "NOT", term: parseExpression(ts, 3) };
    }
    if (token.type === "LPAREN") {
      const expr = parseExpression(ts, 0);
      ts.expect("RPAREN", "Expected ')'");
      return expr;
    }
    if (token.type === "FIELD") {
      if ((token.op === ":" || token.op === "~") && ts.match("LPAREN")) {
        const expr = parseExpression(ts, 0);
        ts.expect("RPAREN", "Expected ')'");
        return scopeField(token.key, expr, token.op);
      }
      const value = token.op === "IN" ? parseInValue(ts) : parsePrimaryValue(ts);
      return { type: "FILTER", key: token.key, op: token.op, value };
    }

    throw ts.error("Expected a term", token);
  }

  function parsePrimaryValue(ts) {
    const next = ts.peek();
    if (next.type === "AT") {
      ts.next();
      const term = parsePrimary(ts);
      let field = null;
      if (ts.peek().type === "WORD") {
        field = ts.next().value;
      }
      return { type: "REF", term, field };
    }
    if (next.type === "WORD") {
      const token = ts.next();
      return { type: "TEXT", value: token.value, kind: "word" };
    }
    if (next.type === "PHRASE") {
      const token = ts.next();
      return { type: "TEXT", value: token.value, kind: "phrase" };
    }
    throw ts.error("Expected a field value", next);
  }

  function parseInValue(ts) {
    const next = ts.peek();
    if (next.type !== "WORD") {
      throw ts.error("Expected interval(...) or duration(...)", next);
    }
    const fnName = String(next.value || "").toLowerCase();
    if (fnName === "interval" || fnName === "duration") return parseInValueFromName(ts, fnName);
    throw ts.error("Expected interval(...) or duration(...)", next);
  }

  function parseInValueFromName(ts, fnName) {
    if (fnName === "interval") return parseIntervalValue(ts);
    if (fnName === "duration") return parseDurationValue(ts);
    throw ts.error("Expected interval(...) or duration(...)", ts.peek());
  }

  function parseIntervalValue(ts) {
    const next = ts.peek();
    if (next.type !== "WORD" || String(next.value || "").toLowerCase() !== "interval") {
      throw ts.error("Expected interval(...)", next);
    }
    ts.next();
    return parseIntervalValueBody(ts);
  }

  function parseIntervalValueBody(ts) {
    ts.expect("LPAREN", "Expected '(' after interval");
    const start = parseExpression(ts, 0);
    ts.expect("COMMA", "Expected ',' in interval");
    const end = parseExpression(ts, 0);
    ts.expect("RPAREN", "Expected ')' after interval");
    return { type: "INTERVAL", start, end };
  }

  function parseDurationValue(ts) {
    const next = ts.peek();
    if (next.type !== "WORD" || String(next.value || "").toLowerCase() !== "duration") {
      throw ts.error("Expected duration(...)", next);
    }
    ts.next();
    return parseDurationValueBody(ts);
  }

  function parseDurationValueBody(ts) {
    ts.expect("LPAREN", "Expected '(' after duration");
    const first = parseExpression(ts, 0);
    ts.expect("COMMA", "Expected ',' in duration");
    const second = parseExpression(ts, 0);
    let third = null;
    if (ts.match("COMMA")) {
      third = parseExpression(ts, 0);
    }
    ts.expect("RPAREN", "Expected ')' after duration");

    if (third) {
      return {
        type: "DURATION",
        start: makeAnd([first, second]),
        end: makeAnd([first, third]),
      };
    }
    return {
      type: "DURATION",
      start: first,
      end: second,
    };
  }

  function scopeField(field, node, op = ":") {
    if (!node) return { type: "EMPTY" };
    if (node.type === "TEXT") {
      return { type: "FILTER", key: field, op, value: node };
    }
    if (node.type === "AND") {
      return makeAnd(node.terms.map((term) => scopeField(field, term, op)));
    }
    if (node.type === "OR") {
      return makeOr(node.terms.map((term) => scopeField(field, term, op)));
    }
    if (node.type === "NOT") {
      return { type: "NOT", term: scopeField(field, node.term, op) };
    }
    return node;
  }

  function makeAnd(nodes) {
    const flat = [];
    for (const n of nodes) (n.type === "AND" ? flat.push(...n.terms) : flat.push(n));
    return flat.length === 1 ? flat[0] : { type: "AND", terms: flat };
  }

  function makeOr(nodes) {
    const flat = [];
    for (const n of nodes) (n.type === "OR" ? flat.push(...n.terms) : flat.push(n));
    return flat.length === 1 ? flat[0] : { type: "OR", terms: flat };
  }

  return {
    getFieldValue,
    toComparable,
    globToRegex,
    tokenizeQuery,
    parseQuery,
    matchFieldTerm,
    matchBareTerm,
    filterObjects,
    filterQueryObjects,
    makePredicate,
    getQueryPredicate,
  };
};

LogApp.searchParser = LogApp.buildSearchParser();
[
  "getFieldValue",
  "toComparable",
  "globToRegex",
  "tokenizeQuery",
  "parseQuery",
  "matchFieldTerm",
  "matchBareTerm",
  "filterObjects",
  "filterQueryObjects",
  "makePredicate",
  "getQueryPredicate",
].forEach((key) => {
  LogApp[key] = LogApp.searchParser[key];
});

LogApp.createSearchWorker = (events = [], options = {}) => {
  if (typeof Worker === "undefined") return null;
  const parserSource = LogApp.buildSearchParser.toString();
  const workerMain = (builderSource) => {
    let EVENTS = [];
    let OPTIONS = {};
    const buildParser = eval("(" + builderSource + ")");
    const parser = buildParser();

    onmessage = (event) => {
      const payload = event.data || {};
      if (payload.type === "init") {
        EVENTS = Array.isArray(payload.events) ? payload.events : [];
        OPTIONS = payload.options || {};
        postMessage({ type: "ready" });
        return;
      }
      if (payload.type === "query") {
        const query = payload.query || "";
        if (!query) {
          const all = EVENTS.map((_, idx) => idx);
          postMessage({ type: "result", id: payload.id, indices: all });
          return;
        }
        const filtered = parser.filterQueryObjects(EVENTS, query, OPTIONS);
        const matchSet = new Set(filtered);
        const indices = [];
        for (let i = 0; i < EVENTS.length; i += 1) {
          if (matchSet.has(EVENTS[i])) indices.push(i);
        }
        postMessage({ type: "result", id: payload.id, indices });
      }
    };
  };
  const workerCode = "(" + workerMain.toString() + ")(" + parserSource + ");";

  const blob = new Blob([workerCode], { type: "application/javascript" });
  const worker = new Worker(URL.createObjectURL(blob));
  worker.postMessage({ type: "init", events, options });
  return worker;
};

LogApp.runSearchQuery = (() => {
  let seq = 0;
  return (worker, query, callback) => {
    if (!worker) return null;
    const id = ++seq;
    const handler = (event) => {
      const payload = event.data || {};
      if (payload.type !== "result" || payload.id !== id) return;
      worker.removeEventListener("message", handler);
      callback(payload.indices || []);
    };
    worker.addEventListener("message", handler);
    worker.postMessage({ type: "query", id, query });
    return id;
  };
})();
