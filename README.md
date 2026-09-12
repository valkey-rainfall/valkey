# try-valkey (GitHub Pages branch)

Built artifacts for the try-valkey page: a real `valkey-server` compiled to WebAssembly, running in the browser.
Source and build scripts: branch `exp/try-valkey-wasm`, `utils/try-valkey-wasm/`. This branch holds only the
deployable bundle; `valkey-server.wasm` / `.mjs` are build outputs of that branch's `build.sh`.

- `tryme.html` -- the sign-on animation, handing off to the terminal
- `index.html` -- the plain terminal
