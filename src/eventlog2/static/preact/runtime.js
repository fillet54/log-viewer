(function () {
  const globalObject = window;
  const preactApi = globalObject.preact || {};
  const hooksApi = globalObject.preactHooks || {};
  const htmApi = globalObject.htm || {};
  const signalsApi = globalObject.preactSignals || {};

  const bindHtml = () => {
    if (typeof htmApi.bind === "function" && typeof preactApi.h === "function") {
      return htmApi.bind(preactApi.h);
    }
    return function missingRuntime() {
      throw new Error(
        "EventLog2UI.html requires local Preact vendor files. Replace the placeholder vendor files with real browser builds."
      );
    };
  };

  const noopRender = function missingRender() {
    throw new Error(
      "EventLog2UI.render requires local Preact vendor files. Replace the placeholder vendor files with real browser builds."
    );
  };

  globalObject.EventLog2UI = {
    html: bindHtml(),
    render: typeof preactApi.render === "function" ? preactApi.render.bind(preactApi) : noopRender,
    hydrate: typeof preactApi.hydrate === "function" ? preactApi.hydrate.bind(preactApi) : noopRender,
    createElement: typeof preactApi.createElement === "function" ? preactApi.createElement.bind(preactApi) : preactApi.h,
    Fragment: preactApi.Fragment || null,
    hooks: {
      useState: hooksApi.useState || null,
      useEffect: hooksApi.useEffect || null,
      useLayoutEffect: hooksApi.useLayoutEffect || null,
      useMemo: hooksApi.useMemo || null,
      useRef: hooksApi.useRef || null,
      useContext: hooksApi.useContext || null,
      useReducer: hooksApi.useReducer || null,
      useCallback: hooksApi.useCallback || null,
    },
    signals: {
      signal: signalsApi.signal || null,
      computed: signalsApi.computed || null,
      effect: signalsApi.effect || null,
      batch: signalsApi.batch || null,
      untracked: signalsApi.untracked || null,
      useSignal: signalsApi.useSignal || null,
      useComputed: signalsApi.useComputed || null,
      useSignalEffect: signalsApi.useSignalEffect || null,
      Signal: signalsApi.Signal || null,
    },
    available:
      typeof preactApi.h === "function" &&
      typeof preactApi.render === "function" &&
      typeof htmApi.bind === "function",
    signalsAvailable: typeof signalsApi.signal === "function",
  };

  if (!globalObject.EventLog2UI.available) {
    console.warn(
      "EventLog2UI loaded without full Preact vendor files. The current app will still run, but Preact components cannot mount yet."
    );
  }
})();
