/* eslint-env node */

import { runTests } from "lib0/testing";
import * as api from "./api.tests.js";
import * as ws from "./ws.tests.js";

runTests({
  api,
  ws,
}).then((success) => {
  process.exit(success ? 0 : 1);
});
