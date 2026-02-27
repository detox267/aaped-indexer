// anchorDecode.js
const anchor = require("@coral-xyz/anchor");

function makeEventDecoder(idl) {
  const coder = new anchor.BorshCoder(idl);

  return function decodeEventsFromLogs(logMessages) {
    const out = [];
    for (const line of logMessages || []) {
      // Anchor events appear as: "Program log: <base64>"
      // coder.events.decode expects the raw log line
      const ev = coder.events.decode(line);
      if (ev) out.push(ev); // { name, data }
    }
    return out;
  };
}

module.exports = { makeEventDecoder };
