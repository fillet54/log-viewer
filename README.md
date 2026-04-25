# Event Log Viewer

Minimal Clojure + ClojureScript hello-world setup for an HTML event log viewer.

Stack:

- Ring + Jetty for the HTTP server
- Reitit for routing
- Component for system lifecycle
- Replicant on the default client path
- Reagent/re-frame isolated under `eventlog.reagent` as the temporary legacy client
- `cljs.main` for ClojureScript compilation

This setup avoids `shadow-cljs` and npm. The legacy Reagent app still uses `cljsjs` React packages so the CLJS builds stay on plain `clojure` tooling.

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

## Replicant layout spike

The Replicant path is the default frontend namespace and app entrypoint.
The legacy Reagent/re-frame app is intentionally isolated under `eventlog.reagent` while it is being phased out.
It uses only Maven/Clojure CLI dependencies and `cljs.main`; do not add npm or `shadow-cljs`.

Compile the default Replicant layout app:

```bash
clojure -M:build-cljs
```

Compile Portfolio scenes:

```bash
clojure -M:portfolio:build-portfolio-cljs
```

Compile the legacy Reagent app:

```bash
clojure -M:build-reagent-cljs
```

Run the server with Portfolio on the classpath:

```bash
clojure -M:portfolio:run
```

Then open:

- `http://localhost:3000/` for the default atom-backed Replicant layout.
- `http://localhost:3000/reagent.html` for the legacy Reagent/re-frame app.
- `http://localhost:3000/portfolio.html` for Portfolio scenes, including expanded and collapsed layout states.

## Structure

- `src/clj/eventlog/main.clj`: app entrypoint
- `src/clj/eventlog/system.clj`: Component system wiring
- `src/clj/eventlog/web.clj`: Ring/Reitit server
- `src/cljs/eventlog/app.cljs`: Replicant app root
- `src/cljs/eventlog/reagent/app.cljs`: legacy Reagent app root
- `resources/public/index.html`: page shell
