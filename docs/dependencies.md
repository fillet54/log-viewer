# Dependencies

Runtime dependencies are Flask (HTTP application), Jinja2 (templates), Waitress (production WSGI serving), and docopt (CLI parsing). The standard library handles storage, parsing, discovery, and bundling. Development-only tools are pytest, flake8, and black.

JavaScript has no npm or bundler build. Browser libraries are pinned CDN distributions committed under `src/cinc/static/vendor/`; changes must update the manifest and its SHA-256 values. Standalone output must be opened from `file://` with networking disabled after every vendor change.
