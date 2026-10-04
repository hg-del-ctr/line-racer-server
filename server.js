const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

let waitingQueue = [];
let nextPeerId = 1;

console.log(`Matchmaking & Signaling server running on port ${PORT}`);

wss.on("connection", (ws) => {
  ws.peerId = nextPeerId++;
  ws.partner = null;
  ws.isHost = false;

  console.log(`Player connected. Assigned ID: ${ws.peerId}`);

  ws.on("message", (rawMessage) => {
    let msg;
    try {
      msg = JSON.parse(rawMessage);
    } catch (e) {
      return;
    }

    switch (msg.type) {
      case "queue":
        _handleQueue(ws);
        break;

      case "signal":
        // Relay WebRTC SDP offers/answers or ICE candidates directly to partner
        if (ws.partner && ws.partner.readyState === ws.OPEN) {
          ws.partner.send(JSON.stringify({
            type: "signal",
            data: msg.data
          }));
        }
        break;
    }
  });

  ws.on("close", () => {
    console.log(`Player disconnected: ${ws.peerId}`);
    waitingQueue = waitingQueue.filter((p) => p !== ws);

    if (ws.partner && ws.partner.readyState === ws.OPEN) {
      ws.partner.send(JSON.stringify({ type: "partner_disconnected" }));
      ws.partner.partner = null;
    }
  });
});

function _handleQueue(ws) {
  // Prevent duplicate queuing
  if (waitingQueue.includes(ws)) return;

  if (waitingQueue.length > 0) {
    // Match found! Pair with the waiting player
    const host = waitingQueue.shift();
    const client = ws;

    host.partner = client;
    host.isHost = true;
    client.partner = host;
    client.isHost = false;

    console.log(`Matched Room: Host (${host.peerId}) <---> Client (${client.peerId})`);

    // Notify Host (acts as the Godot server peer, ID 1)
    host.send(JSON.stringify({
      type: "match_found",
      role: "host",
      peerId: host.peerId
    }));

    // Notify Client (acts as the connecting Godot peer)
    client.send(JSON.stringify({
      type: "match_found",
      role: "client",
      peerId: client.peerId
    }));
  } else {
    // No match yet; wait in queue
    waitingQueue.push(ws);
    console.log(`Player ${ws.peerId} entered matchmaking queue.`);
    ws.send(JSON.stringify({ type: "queued" }));
  }
}