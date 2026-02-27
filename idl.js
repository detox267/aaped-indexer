const fs = require("fs");

const IDL_PATH = process.env.IDL_PATH;
if (!IDL_PATH) throw new Error("Missing IDL_PATH env var");

function loadIdl() {
  const raw = fs.readFileSync(IDL_PATH, "utf8");
  return JSON.parse(raw);
}

module.exports = { loadIdl };
