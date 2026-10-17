const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

let waitingQueue = [];
let nextPeerId = 1;
let nextRoomId = 1;

// Active match rooms stored by ID
const rooms = new Map();

console.log(`Matchmaking & Signaling server running on port ${PORT}`);

wss.on("connection", (ws) => {
  ws.peerId = nextPeerId++;
  ws.room = null;
  ws.partner = null;
  ws.isHost = false;
  ws.playerName = `Player ${ws.peerId}`;

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
        if (msg.name) ws.playerName = msg.name;
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

      case "race_finish":
        _handleRaceFinish(ws, msg);
        break;
    }
  });

  ws.on("close", () => {
    console.log(`Player disconnected: ${ws.peerId}`);
    waitingQueue = waitingQueue.filter((p) => p !== ws);

    const room = ws.room;
    if (room) {
      // If the player disconnected without an official finish time, mark them as DNF
      if (!room.results.has(ws.peerId)) {
        room.results.set(ws.peerId, {
          id: ws.peerId,
          name: ws.playerName,
          finished: false,
          time: 0.0,
          score: -100.0,
          racing: false
        });
      } else {
        const res = room.results.get(ws.peerId);
        res.racing = false;
      }

      // Remove player from active socket list
      room.players = room.players.filter((p) => p !== ws);

      // Notify partner/peers of the disconnection
      if (ws.partner && ws.partner.readyState === ws.OPEN) {
        ws.partner.send(JSON.stringify({ type: "partner_disconnected" }));
      }

      // Re-broadcast standings to remaining players so their boards stay accurate
      _broadcastRoomStandings(room);

      // Clean up room memory once everyone has left
      if (room.players.length === 0) {
        rooms.delete(room.id);
        console.log(`Room ${room.id} cleaned up.`);
      }
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

    const roomId = nextRoomId++;
    const room = {
      id: roomId,
      players: [host, client],
      results: new Map(), // peerId -> result object
      finishPlaceCounter: 0
    };
    rooms.set(roomId, room);

    host.room = room;
    client.room = room;

    host.partner = client;
    host.isHost = true;
    client.partner = host;
    client.isHost = false;

    console.log(`Room ${roomId} created: Host (${host.peerId}) <---> Client (${client.peerId})`);

    // Notify Host (acts as ID 1 in Godot WebRTC mesh)
    host.send(JSON.stringify({
      type: "match_found",
      role: "host",
      peerId: host.peerId,
      roomId: roomId
    }));

    // Notify Client (acts as ID 2 in Godot WebRTC mesh)
    client.send(JSON.stringify({
      type: "match_found",
      role: "client",
      peerId: client.peerId,
      roomId: roomId
    }));
  } else {
    // Wait in queue
    waitingQueue.push(ws);
    console.log(`Player ${ws.peerId} entered matchmaking queue.`);
    ws.send(JSON.stringify({ type: "queued" }));
  }
}

function _handleRaceFinish(ws, msg) {
  const room = ws.room;
  if (!room) return;

  const finished = Boolean(msg.finished);
  const finishTime = Number(msg.finishTime) || 0.0;
  const progressScore = Number(msg.progressScore) || 0.0;
  const playerName = msg.name || ws.playerName;

  let assignedRank = null;
  if (finished) {
    room.finishPlaceCounter += 1;
    assignedRank = room.finishPlaceCounter;

    // Immediately inform this player of their official rank so confetti & UI can fire
    if (ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify({
        type: "placement_announced",
        rank: assignedRank
      }));
    }
  }

  room.results.set(ws.peerId, {
    id: ws.peerId,
    name: playerName,
    finished: finished,
    time: finishTime,
    score: progressScore,
    racing: false
  });

  console.log(`Room ${room.id}: Player ${ws.peerId} finished: ${finished} (Time: ${finishTime}s)`);

  _broadcastRoomStandings(room);
}

function _broadcastRoomStandings(room) {
  const standings = Array.from(room.results.values());

  // Sort standings: Finished by lowest time first; Unfinished by highest score
  standings.sort((a, b) => {
    if (a.finished && b.finished) return a.time - b.time;
    if (a.finished !== b.finished) return a.finished ? -1 : 1;
    return b.score - a.score;
  });

  // Append any players still actively racing
  for (const playerWs of room.players) {
    if (!room.results.has(playerWs.peerId)) {
      standings.push({
        id: playerWs.peerId,
        name: playerWs.playerName,
        finished: false,
        time: 0.0,
        score: -1.0,
        racing: true
      });
    }
  }

  const payload = JSON.stringify({
    type: "standings_update",
    standings: standings
  });

  for (const playerWs of room.players) {
    if (playerWs.readyState === playerWs.OPEN) {
      playerWs.send(payload);
    }
  }
}
