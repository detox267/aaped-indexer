const fs = require("fs");
const path = require("path");

function loadIdl(idlPath = process.env.IDL_PATH) {
  if (!idlPath) throw new Error("Missing IDL_PATH env var");

  const resolved = path.resolve(idlPath);
  if (!fs.existsSync(resolved)) {
    throw new Error(`IDL_PATH not found: ${resolved}`);
  }

  const raw = fs.readFileSync(resolved, "utf8");
  const idl = JSON.parse(raw);

  if (!idl.name && idl.metadata?.name) {
    idl.name = idl.metadata.name;
  }

  if (!Array.isArray(idl.events)) {
    idl.events = [];
  }

  if (!Array.isArray(idl.instructions)) {
    idl.instructions = [];
  }

  if (!Array.isArray(idl.types)) {
    idl.types = [];
  }

  return idl;
}

module.exports = { loadIdl };
