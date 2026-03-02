// server.js
require("dotenv").config();
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const { startIndexer } = require("./indexer");

// --------------------
// HTTP + Socket.IO (DOWNSTREAM)
// --------------------
const app = express();
app.use(express.json({ limit: "10mb" }));

app.use((req, res, next) => {
  const allowed = ["https://aaped.fun", "https://www.aaped.fun"];
  const origin = req.headers.origin;

  if (allowed.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }

  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Allow-Credentials", "true");

  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

app.get("/health", (req, res) => res.json({ ok: true }));

const server = http.createServer(app);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

// Room contract (clients -> join/leave)
io.on("connection", (socket) => {
  // Join a mint-scoped room group
  // Example: socket.emit("joinMint", { mint: "<mint>" })
  socket.on("joinMint", ({ mint }) => {
    if (mint && typeof mint === "string") {
      socket.join(`mint:${mint}`);
      socket.emit("joined", { room: `mint:${mint}` });
    }
  });

  socket.on("leaveMint", ({ mint }) => {
    if (mint && typeof mint === "string") {
      socket.leave(`mint:${mint}`);
      socket.emit("left", { room: `mint:${mint}` });
    }
  });

  // Join specific topic rooms (optional)
  // Example: socket.emit("joinRoom", { room: "global:trades" })
  socket.on("joinRoom", ({ room }) => {
    if (room && typeof room === "string") {
      socket.join(room);
      socket.emit("joined", { room });
    }
  });

  socket.on("leaveRoom", ({ room }) => {
    if (room && typeof room === "string") {
      socket.leave(room);
      socket.emit("left", { room });
    }
  });

  // Convenience: join all globals
  socket.on("joinGlobals", () => {
    const rooms = ["global:launch", "global:trades", "global:migration", "global:prices", "global:events"];
    rooms.forEach((r) => socket.join(r));
    socket.emit("joinedGlobals", { rooms });
  });
});

// Start indexer (UPSTREAM) and give it `io` to emit to rooms
(async () => {
  await startIndexer({ io });

  const PORT = Number(process.env.PORT || 3000);
  server.listen(PORT, () => {
    console.log(`HTTP+Socket server listening on :${PORT}`);
    console.log(`Socket.IO path: /socket.io  (same port)`);
  });
})();
