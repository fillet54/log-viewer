window.LogApp = window.LogApp || {};

LogApp.buildSearchParser = () => {
  const getFieldValue = (event, path) => {
    if (!event || !path) return null;
    const parts = path.split(".");
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

    return objects.filter((obj) => evaluate(ast, obj));

    function evaluate(node, obj) {
      if (!node) return true;
      switch (node.type) {
        case "EMPTY":
          return true;
        case "AND":
          return node.terms.every((t) => evaluate(t, obj));
        case "OR":
          return node.terms.some((t) => evaluate(t, obj));
        case "NOT":
          return !evaluate(node.term, obj);
        case "TEXT":
          return matchBareTerm(obj, node.value);
        case "FILTER":
          return evalFilter(node, obj);
        default:
          return true;
      }
    }

    function evalFilter(node, obj) {
      const key = node.key.toLowerCase();

      if (fieldHandlers[key]) {
        return fieldHandlers[key](obj, node.value, evaluate, node.op);
      }

      return defaultFieldEval(key, node.value, obj, node.op);
    }

    function defaultFieldEval(key, valueNode, obj, op) {
      if (!valueNode || valueNode.type !== "TEXT") return false;
      if (valueNode.value == null) return false;
      if (key === "$" && op && op !== ":" && op !== "~") return false;
      if (!op || op === ":") {
        return matchFieldTerm(obj, key, valueNode.value);
      }
      if (op === "~") {
        return containsField(obj, key, valueNode.value);
      }
      if (![">", ">=", "<", "<="].includes(op)) return false;
      return compareField(obj, key, valueNode.value, op);
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
      return { type: "WORD", value: word, start: wordStart, end: i };
    };

    const isWhitespace = (c) => c === " " || c === "\t" || c === "\n" || c === "\r";

    const isWordChar = (c) =>
      c !== undefined &&
      !isWhitespace(c) &&
      c !== "(" &&
      c !== ")" &&
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
      if (ts.peek().type === "COMP" || ts.peek().type === "CONTAINS") {
        const comp = ts.next();
        const value = parsePrimaryValue(ts);
        return { type: "FILTER", key: token.value, op: comp.op, value };
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
      const value = parsePrimaryValue(ts);
      return { type: "FILTER", key: token.key, op: token.op, value };
    }

    throw ts.error("Expected a term", token);
  }

  function parsePrimaryValue(ts) {
    const next = ts.peek();
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
  "makePredicate",
  "getQueryPredicate",
].forEach((key) => {
  LogApp[key] = LogApp.searchParser[key];
});

LogApp.createSearchWorker = (events = []) => {
  if (typeof Worker === "undefined") return null;
  const parserSource = LogApp.buildSearchParser.toString();
  const workerMain = (builderSource) => {
    let EVENTS = [];
    const buildParser = eval("(" + builderSource + ")");
    const parser = buildParser();

    onmessage = (event) => {
      const payload = event.data || {};
      if (payload.type === "init") {
        EVENTS = Array.isArray(payload.events) ? payload.events : [];
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
        const predicate = parser.makePredicate(query);
        const indices = [];
        for (let i = 0; i < EVENTS.length; i += 1) {
          if (predicate(EVENTS[i])) indices.push(i);
        }
        postMessage({ type: "result", id: payload.id, indices });
      }
    };
  };
  const workerCode = "(" + workerMain.toString() + ")(" + parserSource + ");";

  const blob = new Blob([workerCode], { type: "application/javascript" });
  const worker = new Worker(URL.createObjectURL(blob));
  worker.postMessage({ type: "init", events });
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
