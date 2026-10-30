/** @type {import("eslint").Linter.FlatConfig[]} */
module.exports = [
  {
    files: ["frontend/**/*.js"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: {
        window: "readonly",
        document: "readonly",
        console: "readonly",
        fetch: "readonly",
        navigator: "readonly",
        Image: "readonly",
        WebGLRenderingContext: "readonly",
        WebGL2RenderingContext: "readonly",
        localStorage: "readonly",
        sessionStorage: "readonly",
        URL: "readonly",
        Blob: "readonly",
        FileReader: "readonly",
        setTimeout: "readonly",
        clearTimeout: "readonly",
        setInterval: "readonly",
        clearInterval: "readonly",
        performance: "readonly",
        AbortController: "readonly",
        URLSearchParams: "readonly",
        HTMLElement: "readonly",
        XMLHttpRequest: "readonly",
        FormData: "readonly"
      }
    },
    rules: {
      "no-unused-vars": ["warn", { "argsIgnorePattern": "^_", "varsIgnorePattern": "^_" }],
      "no-console": ["warn", { "allow": ["warn", "error", "info"] }],
      "eqeqeq": ["error", "always", { "null": "ignore" }],
      "no-var": "error",
      "prefer-const": "warn",
      "no-undef": "error",
      "no-constant-condition": ["error", { "checkLoops": false }],
      "no-debugger": "warn",
      "no-duplicate-imports": "error",
      "no-unreachable": "error"
    }
  },
  {
    // Pure helpers (frontend/AGENTS.md, "Layers"): inputs in, outputs out, so
    // they can be tested without a page. No DOM, storage or network, and no
    // imports from the layers above them.
    files: ["frontend/modules/*_utils.js", "frontend/modules/*_engine.js"],
    rules: {
      "no-restricted-globals": [
        "error",
        { name: "document", message: "Pure helpers do not touch the DOM; do it in a controller." },
        { name: "window", message: "Pure helpers do not touch the page; pass what you need in." },
        { name: "localStorage", message: "Pure helpers keep no state; storage belongs in a controller." },
        { name: "sessionStorage", message: "Pure helpers keep no state; storage belongs in a controller." },
        { name: "fetch", message: "Pure helpers make no requests; call the backend from a controller." }
      ],
      "no-restricted-imports": [
        "error",
        {
          patterns: [
            { group: ["./*_controller.js", "./*_bindings.js"], message: "Pure helpers do not depend on controllers or bindings." },
            { group: ["../app.js", "./state.js"], message: "Pure helpers get their inputs as arguments, not from app state." }
          ]
        }
      ]
    }
  }
];
