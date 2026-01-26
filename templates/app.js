{# JS bundle rendered via Jinja to keep components co-located #}
{% include 'components/layout.js' %}

{% include 'components/log_view.js' %}

{% include 'components/top_chart.js' %}

{% include 'components/search_pane.js' %}

{% include 'components/right_pane.js' %}

window.LogApp = window.LogApp || {};

LogApp.STORAGE_KEYS = {
  root: "loglayout.split.root",
  top: "loglayout.split.top",
  bottomCollapsed: "loglayout.split.bottom.collapsed",
  search: "loglayout.split.search",
  rootExpanded: "loglayout.split.root.expanded",
  searchHistory: "loglayout.search.history",
  searchPinned: "loglayout.search.pinned",
  searchFilters: "loglayout.search.filters",
  chartTooltips: "loglayout.chart.tooltips",
};

LogApp.createEventBus = () => {
  const listeners = new Map();
  return {
    on(event, handler) {
      if (!listeners.has(event)) listeners.set(event, new Set());
      listeners.get(event).add(handler);
      return () => listeners.get(event)?.delete(handler);
    },
    emit(event, payload) {
      const handlers = listeners.get(event);
      if (!handlers) return;
      handlers.forEach((handler) => handler(payload));
    },
  };
};

LogApp.loadSizes = (key, fallback) => {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed) || parsed.length !== fallback.length) return fallback;
    if (parsed.some((value) => typeof value !== "number")) return fallback;
    return parsed;
  } catch (err) {
    return fallback;
  }
};

LogApp.saveSizes = (key, sizes) => {
  localStorage.setItem(key, JSON.stringify(sizes));
};

LogApp.loadLogData = () => {
  const logPayload = document.getElementById("log-data");
  if (!logPayload) return null;
  try {
    return JSON.parse(logPayload.textContent || "{}");
  } catch (err) {
    return null;
  }
};

LogApp.smoothScrollTo = (container, targetTop, durationMs = 200, onComplete = null) => {
  const startTop = container.scrollTop;
  const delta = targetTop - startTop;
  if (Math.abs(delta) < 2) {
    container.scrollTop = targetTop;
    if (onComplete) onComplete();
    return;
  }
  const startTime = performance.now();
  const easeOut = (t) => 1 - Math.pow(1 - t, 3);
  const step = (now) => {
    const elapsed = now - startTime;
    const progress = Math.min(1, elapsed / durationMs);
    container.scrollTop = startTop + delta * easeOut(progress);
    if (progress < 1) {
      requestAnimationFrame(step);
    } else if (onComplete) {
      onComplete();
    }
  };
  requestAnimationFrame(step);
};

LogApp.getFieldValue = (event, path) => {
  if (!event || !path) return null;
  const parts = path.split(".");
  let current = event;
  for (const part of parts) {
    if (current == null || typeof current !== "object") return null;
    current = current[part];
  }
  return current;
};

LogApp.toComparable = (value) => {
  if (value == null) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
};

LogApp.globToRegex = (pattern) => {
  const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, "\\$&");
  const regex = "^" + escaped.replace(/\*/g, ".*") + "$";
  return new RegExp(regex, "i");
};

LogApp.tokenizeQuery = (input) => {
  const tokens = [];
  let i = 0;
  while (i < input.length) {
    const ch = input[i];
    if (/\s/.test(ch)) {
      i += 1;
      continue;
    }
    if (ch === "(" || ch === ")") {
      tokens.push({ type: ch });
      i += 1;
      continue;
    }
    if (ch === '"') {
      let j = i + 1;
      let value = "";
      while (j < input.length && input[j] !== '"') {
        value += input[j];
        j += 1;
      }
      tokens.push({ type: "TERM", value });
      i = j + 1;
      continue;
    }
    let value = "";
    while (i < input.length && !/\s|\(|\)/.test(input[i])) {
      value += input[i];
      i += 1;
    }
    if (value.toUpperCase() === "OR" || value === "|") {
      tokens.push({ type: "OR" });
    } else {
      tokens.push({ type: "TERM", value });
    }
  }
  return tokens;
};

LogApp.parseQuery = (input) => {
  const tokens = LogApp.tokenizeQuery(input || "");
  let idx = 0;

  const peek = () => tokens[idx];
  const consume = () => tokens[idx++];

  const parseExpression = () => {
    let node = parseTerm();
    while (peek() && peek().type === "OR") {
      consume();
      node = { type: "OR", left: node, right: parseTerm() };
    }
    return node;
  };

  const parseTerm = () => {
    const factors = [];
    while (peek() && peek().type !== "OR" && peek().type !== ")") {
      factors.push(parseFactor());
    }
    if (factors.length === 1) return factors[0];
    return { type: "AND", items: factors };
  };

  const parseFactor = () => {
    const token = peek();
    if (!token) return { type: "EMPTY" };
    if (token.type === "TERM" && token.value.startsWith("-")) {
      consume();
      const value = token.value.slice(1);
      return { type: "NOT", node: parseAtomFromValue(value) };
    }
    if (token.type === "(") {
      consume();
      const node = parseExpression();
      if (peek() && peek().type === ")") consume();
      return node;
    }
    if (token.type === "TERM") {
      consume();
      return parseAtomFromValue(token.value);
    }
    consume();
    return { type: "EMPTY" };
  };

  const parseAtomFromValue = (value) => {
    const colonIndex = value.indexOf(":");
    if (colonIndex > 0) {
      const field = value.slice(0, colonIndex);
      const term = value.slice(colonIndex + 1);
      return { type: "FIELD", field, term };
    }
    return { type: "BARE", term: value };
  };

  if (!tokens.length) return { type: "EMPTY" };
  return parseExpression();
};

LogApp.matchFieldTerm = (event, field, term) => {
  const value = LogApp.getFieldValue(event, field);
  if (value == null) return false;
  const text = LogApp.toComparable(value);
  if (term.includes("*")) {
    return LogApp.globToRegex(term).test(text);
  }
  return text.toLowerCase() === term.toLowerCase();
};

LogApp.matchBareTerm = (event, term) => {
  if (!term) return true;
  const nameValue = LogApp.toComparable(event?.name || "");
  if (LogApp.globToRegex(`${term}*`).test(nameValue)) return true;
  const entries = Object.entries(event || {});
  for (const [key, value] of entries) {
    if (key === "data") continue;
    const text = LogApp.toComparable(value);
    if (text && text.toLowerCase() === term.toLowerCase()) return true;
  }
  return false;
};

LogApp.matchesQuery = (event, query) => {
  const ast = LogApp.parseQuery(query);
  const evalNode = (node) => {
    if (!node) return true;
    switch (node.type) {
      case "EMPTY":
        return true;
      case "OR":
        return evalNode(node.left) || evalNode(node.right);
      case "AND":
        return node.items.every(evalNode);
      case "NOT":
        return !evalNode(node.node);
      case "FIELD":
        return LogApp.matchFieldTerm(event, node.field, node.term);
      case "BARE":
        return LogApp.matchBareTerm(event, node.term);
      default:
        return true;
    }
  };
  return evalNode(ast);
};

LogApp.getQueryPredicate = (() => {
  const cache = new Map();
  return (query) => {
    const key = query || "";
    if (cache.has(key)) return cache.get(key);
    const ast = LogApp.parseQuery(key);
    const predicate = (event) => {
      const evalNode = (node) => {
        if (!node) return true;
        switch (node.type) {
          case "EMPTY":
            return true;
          case "OR":
            return evalNode(node.left) || evalNode(node.right);
          case "AND":
            return node.items.every(evalNode);
          case "NOT":
            return !evalNode(node.node);
          case "FIELD":
            return LogApp.matchFieldTerm(event, node.field, node.term);
          case "BARE":
            return LogApp.matchBareTerm(event, node.term);
          default:
            return true;
        }
      };
      return evalNode(ast);
    };
    cache.set(key, predicate);
    return predicate;
  };
})();

LogApp.createSearchWorker = (events = []) => {
  if (typeof Worker === "undefined") return null;
  const workerMain = () => {
    let EVENTS = [];

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
      let i = 0;
      while (i < input.length) {
        const ch = input[i];
        if (/\s/.test(ch)) {
          i += 1;
          continue;
        }
        if (ch === "(" || ch === ")") {
          tokens.push({ type: ch });
          i += 1;
          continue;
        }
        if (ch === '"') {
          let j = i + 1;
          let value = "";
          while (j < input.length && input[j] !== '"') {
            value += input[j];
            j += 1;
          }
          tokens.push({ type: "TERM", value });
          i = j + 1;
          continue;
        }
        let value = "";
        while (i < input.length && !/\s|\(|\)/.test(input[i])) {
          value += input[i];
          i += 1;
        }
        if (value.toUpperCase() === "OR" || value === "|") {
          tokens.push({ type: "OR" });
        } else {
          tokens.push({ type: "TERM", value });
        }
      }
      return tokens;
    };

    const parseQuery = (input) => {
      const tokens = tokenizeQuery(input || "");
      let idx = 0;

      const peek = () => tokens[idx];
      const consume = () => tokens[idx++];

      const parseExpression = () => {
        let node = parseTerm();
        while (peek() && peek().type === "OR") {
          consume();
          node = { type: "OR", left: node, right: parseTerm() };
        }
        return node;
      };

      const parseTerm = () => {
        const factors = [];
        while (peek() && peek().type !== "OR" && peek().type !== ")") {
          factors.push(parseFactor());
        }
        if (factors.length === 1) return factors[0];
        return { type: "AND", items: factors };
      };

      const parseFactor = () => {
        const token = peek();
        if (!token) return { type: "EMPTY" };
        if (token.type === "TERM" && token.value.startsWith("-")) {
          consume();
          const value = token.value.slice(1);
          return { type: "NOT", node: parseAtomFromValue(value) };
        }
        if (token.type === "(") {
          consume();
          const node = parseExpression();
          if (peek() && peek().type === ")") consume();
          return node;
        }
        if (token.type === "TERM") {
          consume();
          return parseAtomFromValue(token.value);
        }
        consume();
        return { type: "EMPTY" };
      };

      const parseAtomFromValue = (value) => {
        const colonIndex = value.indexOf(":");
        if (colonIndex > 0) {
          const field = value.slice(0, colonIndex);
          const term = value.slice(colonIndex + 1);
          return { type: "FIELD", field, term };
        }
        return { type: "BARE", term: value };
      };

      if (!tokens.length) return { type: "EMPTY" };
      return parseExpression();
    };

    const matchFieldTerm = (event, field, term) => {
      const value = getFieldValue(event, field);
      if (value == null) return false;
      const text = toComparable(value);
      if (term.includes("*")) {
        return globToRegex(term).test(text);
      }
      return text.toLowerCase() === term.toLowerCase();
    };

    const matchBareTerm = (event, term) => {
      if (!term) return true;
      const nameValue = toComparable(event?.name || "");
      if (globToRegex(term + "*").test(nameValue)) return true;
      const entries = Object.entries(event || {});
      for (const [key, value] of entries) {
        if (key === "data") continue;
        const text = toComparable(value);
        if (text && text.toLowerCase() === term.toLowerCase()) return true;
      }
      return false;
    };

    const makePredicate = (query) => {
      const ast = parseQuery(query);
      const evalNode = (node, event) => {
        if (!node) return true;
        switch (node.type) {
          case "EMPTY":
            return true;
          case "OR":
            return evalNode(node.left, event) || evalNode(node.right, event);
          case "AND":
            return node.items.every((item) => evalNode(item, event));
          case "NOT":
            return !evalNode(node.node, event);
          case "FIELD":
            return matchFieldTerm(event, node.field, node.term);
          case "BARE":
            return matchBareTerm(event, node.term);
          default:
            return true;
        }
      };
      return (event) => evalNode(ast, event);
    };

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
        const predicate = makePredicate(query);
        const indices = [];
        for (let i = 0; i < EVENTS.length; i += 1) {
          if (predicate(EVENTS[i])) indices.push(i);
        }
        postMessage({ type: "result", id: payload.id, indices });
      }
    };
  };
  const workerCode = "(" + workerMain.toString() + ")();";

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

window.addEventListener("DOMContentLoaded", () => {
  const bus = LogApp.createEventBus();
  const logData = LogApp.loadLogData();
  LogApp.searchWorker = LogApp.createSearchWorker(logData?.events || []);

  LogApp.initSplits();
  LogApp.initLogList(logData, bus);
  LogApp.initChart(logData, bus);
  LogApp.initSearchPane(logData, bus);
  LogApp.initRightPane(bus);
});
