# Frontend notes for coding agents

Read the root [AGENTS.md](../AGENTS.md) first; this adds what applies inside
`frontend/`. The interface rules — tokens, controls, dialogs, how to check a
visible change — are in [docs/UI_GUIDE.md](../docs/UI_GUIDE.md).

No framework, no bundler, no build step: `index.html` loads `app.js` as an ES
module, which imports `modules/*.js` by relative path (with the `.js`
extension). What is in the folder is what the browser runs.

## Layers

Read [DEVELOPER_GUIDE.md › Frontend Mental Model](../docs/DEVELOPER_GUIDE.md#frontend-mental-model-for-new-contributors)
— the diagram, the two wiring patterns and the worked example. In short:

| Layer | Files | May | May not |
| --- | --- | --- | --- |
| State | `modules/state.js` | Define the state objects. | Hold logic. |
| Pure helpers | `*_utils.js`, `*_engine.js`, small single-purpose modules (`zoom_slider.js`) | Take inputs, return outputs. | Touch the DOM, `window`, storage or the network; import controllers or bindings. eslint enforces this for `*_utils.js` and `*_engine.js`. |
| Controllers | `*_controller.js` | Own a domain: its DOM elements, its slice of `state`, its methods. | Attach listeners for other domains. |
| Bindings | `*_bindings.js` | Attach listeners and forward to controllers or `app.js` callbacks. | Decide anything — logic found in a binding belongs in a controller. |
| Composition root | `app.js` | Own `state`, grab DOM references, define stable wrapper functions, create everything in order. | Grow new logic: put it in a module and wire it here. |

A controller or binding gets what it needs as `{ state, elements, callbacks }`
from `app.js` (often through `app_binding_contexts.js`). Only `t()`
(`i18n.js`), `notify*` (`toast.js`) and `dialogs.js` are imported directly
everywhere.

## Where does it go

| You are adding | Touch |
| --- | --- |
| **A calculation or transform** | A pure helper, with a test that needs no DOM. |
| **A control or a feature** | Markup in `index.html` (`data-i18n*` on every string), its id in `modules/app_elements.js` and its name in `app.js`'s list from it if `app.js` needs the element, behaviour on the owning controller, the listener in the matching `*_bindings.js`, wiring in `app.js`, styles in the area's file in `styles/` from the tokens (see the UI guide), keys in all 13 locales, a test. The worked example in the developer guide walks through it. |
| **A dialog** | The `.modal` structure and layout in the UI guide; open and close it through the existing modal handling, so Esc and the backdrop work. |
| **A call to the backend** | `fetchJSON` / `fetchJSONWithInit` from `modules/http.js`, or `fetchResponse` when the answer is bytes, headers or a 204; all turn a failure into a translated message, with `status` and the server's `detail` on the error. `native_picker.js` for the system's file and folder dialogs. |
| **A setting** | Backend first (see [backend/AGENTS.md](../backend/AGENTS.md)), then the Settings dialog's tab, its keys, the user guide. |
| **A user-visible message** | `setStatus(…, { tone })` for the status line, `notify*` for a toast, `dialogs.js` to ask. Never `alert()`. |

## Rules

- **No new runtime dependency and nothing from a CDN.** ALBIS runs offline. A
  library is vendored into `frontend/vendor/` with its licence, and listed in
  [THIRD_PARTY_LICENSES.md](../THIRD_PARTY_LICENSES.md).
- **Strings**: `data-i18n`, `data-i18n-title`, `data-i18n-aria-label`,
  `data-i18n-placeholder` in markup, `t("key", { vars })` in JS; the key in all
  13 locales.
- **Stable wrappers**: when one module must call another created later, add a
  named function in `app.js` and pass that, not the controller's method. They
  look like boilerplate and are deliberate; see
  [decision 0005](../docs/decisions/0005-app-elements-table-and-stable-wrappers.md).
- **State lives in `state` or a controller**, not in the DOM: read values from
  `state`, write the DOM from it.
- `console.warn`/`error`/`info` only; eslint flags `console.log`.

## Tests

Vitest with jsdom, in `frontend/tests/`, one file per module
(`zoom_slider.test.js` for `zoom_slider.js`). Build only the DOM a test needs
in `document.body.innerHTML`, pass fakes for callbacks (`vi.fn()`), and assert
on what the user would see or what the module calls. Run them with
`npm run test:js`; the style guard and the locale tests run with them.

## Copy these

- **Pure helper**: `modules/zoom_slider.js` and `modules/file_type_utils.js`,
  with their tests.
- **Controller**: `modules/ring_list_controller.js` with
  `tests/ring_list_controller.test.js`.
- **Bindings**: `modules/colorbar_window_bindings.js` with its test.
- **A self-contained widget**: `modules/mask_legend.js`.

Do not copy `app.js`'s size. `tests/code_size_guard.test.js` holds it: a file
over 1,000 lines may not get longer. The same test fails on a `fetch()` outside
`http.js` (and `i18n.js`, which loads the locales).
