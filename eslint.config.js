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
  }
];
