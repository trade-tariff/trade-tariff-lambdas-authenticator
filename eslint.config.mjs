import { defineConfig } from "eslint/config";

export default defineConfig([
  {
    files: ["**/*.js"],
    languageOptions: { sourceType: "commonjs" },
  },
]);
