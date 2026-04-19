# Event Log Viewer

Minimal Clojure + ClojureScript hello-world setup for an HTML event log viewer.

Stack:

- Ring + Jetty for the HTTP server
- Reitit for routing
- Component for system lifecycle
- Reagent on the client
- `cljs.main` for ClojureScript compilation

This setup avoids `shadow-cljs`. For this first pass, Reagent uses the `cljsjs` React packages so the CLJS build stays simple with plain `clojure` tooling. You can still load other browser libraries from CDNs in `resources/public/index.html`.

## Run

In one terminal, compile the frontend:

```bash
clojure -M:build-cljs
```

For iterative frontend work:

```bash
clojure -M:watch-cljs
```

In another terminal, start the server:

```bash
clojure -M:run
```

Then open `http://localhost:3000`.

## Structure

- `src/clj/eventlog/main.clj`: app entrypoint
- `src/clj/eventlog/system.clj`: Component system wiring
- `src/clj/eventlog/web.clj`: Ring/Reitit server
- `src/cljs/eventlog/app.cljs`: Reagent app root
- `resources/public/index.html`: page shell
