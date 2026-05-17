const anchor = require("@coral-xyz/anchor");

function makeEventDecoder(idl) {
  const coder = new anchor.BorshCoder(idl);

  return function decodeEventsFromLogs(logMessages = []) {
    const events = [];

    for (const line of logMessages || []) {
      const candidates = [];

      if (typeof line !== "string") continue;

      if (line.startsWith("Program data: ")) {
        candidates.push(line.slice("Program data: ".length).trim());
      }

      if (line.startsWith("Program log: ")) {
        candidates.push(line.slice("Program log: ".length).trim());
      }

      candidates.push(line);

      for (const candidate of candidates) {
        try {
          const decoded = coder.events.decode(candidate);
          if (decoded) {
            events.push(decoded);
            break;
          }
        } catch (_) {}
      }
    }

    return events;
  };
}

module.exports = { makeEventDecoder };
