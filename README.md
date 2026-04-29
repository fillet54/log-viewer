# Event Log Viewer

Minimal Clojure + ClojureScript hello-world setup for an HTML event log viewer.

Stack:

- Ring + Jetty for the HTTP server
- Reitit for routing
- Component for system lifecycle
- Replicant on the default client path
- Figwheel Main for active frontend development

This setup avoids `shadow-cljs` and npm.

## Run

Start the server and Figwheel with:

```bash
./scripts/dev
```

Open:

- `http://localhost:3000/` for the main Replicant app
- `http://localhost:3000/portfolio.html` for Portfolio component development

This uses one foreground build (`main-dev`) and one background build (`portfolio-dev`) so both pages hot reload from the same Figwheel process. This follows Figwheel's background-build model: https://figwheel.org/docs/background_builds.html

If you want to run the pieces manually:

```bash
clojure -M:portfolio:run
```

```bash
clojure -M:figwheel -bb portfolio-dev -b main-dev -r
```

## Optional one-off compile

```bash
clojure -M:build-cljs
```

This is only for a one-time app compile. Portfolio is dev-only and is served through Figwheel, not a separate bundled build.

## Structure

- `src/clj/eventlog/main.clj`: app entrypoint
- `src/clj/eventlog/system.clj`: Component system wiring
- `src/clj/eventlog/web.clj`: Ring/Reitit server
- `src/cljs/eventlog/app.cljs`: Replicant app root
- `src/cljs/eventlog/portfolio.cljs`: Portfolio entrypoint
- `resources/public/index.html`: page shell
- `resources/public/portfolio.html`: Portfolio page shell
