/** @type {import("eslint").Linter.FlatConfig[]} */
module.exports = [
  {
    files: ["frontend/**/*.js"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module"
    },
    rules: {}
  }
];
