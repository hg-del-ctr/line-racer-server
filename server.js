const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

let waitingQueue = [];
let queueTimer = null;
let queueCountdown = 0;

let nextPeerId = 1;
let nextRoomId = 1;

const rooms = new Map();

console.log(`Matchmaking & Signaling server running on port ${PORT}`);

wss.on("connection", (ws) => {
  ws.peerId = nextPeerId++;
  ws.room = null;
  ws.meshId = 0; // 1 = Host, 2..4 = Clients
  ws.playerName = `Player ${ws.peerId}`;
  ws.skinId = "classic_red";

  console.log(`Player connected. Assigned Server ID: ${ws.peerId}`);

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
        if (msg.skin_id) ws.skinId = msg.skin_id;
        _handleQueue(ws);
        break;

      case "signal":
        // Route WebRTC offer/answer/ICE to the specific target peer in the room
        if (ws.room && msg.data) {
          const targetMeshId = msg.data.target_id;
          const targetPeer = ws.room.players.find((p) => p.meshId === targetMeshId);
          if (targetPeer && targetPeer.readyState === targetPeer.OPEN) {
            targetPeer.send(JSON.stringify({
              type: "signal",
              data: {
                ...msg.data,
                from_id: ws.meshId
              }
            }));
          }
        }
        break;

      case "race_finish":
        _handleRaceFinish(ws, msg);
        break;
    }
  });

  ws.on("close", () => {
    console.log(`Player disconnected: ${ws.peerId}`);

    // 1. If they were in the matchmaking queue
    const wasInQueue = waitingQueue.includes(ws);
    waitingQueue = waitingQueue.filter((p) => p !== ws);
    if (wasInQueue) {
      _evaluateQueueState();
    }

    // 2. If they were in an active race room
    const room = ws.room;
    if (room) {
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
        room.results.get(ws.peerId).racing = false;
      }

      room.players = room.players.filter((p) => p !== ws);

      for (const peer of room.players) {
        if (peer.readyState === peer.OPEN) {
          peer.send(JSON.stringify({
            type: "partner_disconnected",
            meshId: ws.meshId
          }));
        }
      }

      _broadcastRoomStandings(room);

      if (room.players.length === 0) {
        rooms.delete(room.id);
        console.log(`Room ${room.id} cleaned up.`);
      }
    }
  });
});

function _handleQueue(ws) {
  if (waitingQueue.includes(ws)) return;
  waitingQueue.push(ws);
  console.log(`Player ${ws.peerId} (${ws.playerName}, skin: ${ws.skinId}) joined queue. Total waiting: ${waitingQueue.length}`);
  
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify({
      type: "queued",
      peerId: ws.peerId
    }));
  }

  _evaluateQueueState();
}

function _evaluateQueueState() {
  // Clean out any closed sockets
  waitingQueue = waitingQueue.filter((p) => p.readyState === p.OPEN);

  // If 4+ players are waiting, launch a full 4-player match immediately
  if (waitingQueue.length >= 4) {
    if (queueTimer) {
      clearInterval(queueTimer);
      queueTimer = null;
    }
    _launchMatchFromQueue();
    return;
  }

  // If 2 or 3 players are waiting, start the 5-second countdown
  if (waitingQueue.length >= 2) {
    if (!queueTimer) {
      queueCountdown = 5;
      _broadcastQueueStatus();
      queueTimer = setInterval(() => {
        queueCountdown -= 1;
        if (queueCountdown <= 0) {
          clearInterval(queueTimer);
          queueTimer = null;
          _launchMatchFromQueue();
        } else {
          _broadcastQueueStatus();
        }
      }, 1000);
    } else {
      _broadcastQueueStatus();
    }
  } else {
    if (queueTimer) {
      clearInterval(queueTimer);
      queueTimer = null;
    }
    queueCountdown = 0;
    _broadcastQueueStatus();
  }
}

function _broadcastQueueStatus() {
  const playersData = waitingQueue.map((ws) => ({
    id: ws.peerId,
    name: ws.playerName,
    skin_id: ws.skinId || "classic_red"
  }));

  const payload = JSON.stringify({
    type: "queue_status",
    count: waitingQueue.length,
    countdown: queueCountdown,
    players: playersData
  });

  for (const ws of waitingQueue) {
    if (ws.readyState === ws.OPEN) {
      ws.send(payload);
    }
  }
}

function _launchMatchFromQueue() {
  if (waitingQueue.length < 2) {
    _evaluateQueueState();
    return;
  }

  const matchedPlayers = waitingQueue.splice(0, 4);
  const roomId = nextRoomId++;

  const room = {
    id: roomId,
    players: matchedPlayers,
    results: new Map(),
    finishPlaceCounter: 0
  };
  rooms.set(roomId, room);

  const meshIds = [];
  matchedPlayers.forEach((ws, idx) => {
    ws.room = room;
    ws.meshId = idx + 1; // 1 = Host, 2, 3, 4 = Clients
    ws.isHost = (ws.meshId === 1);
    meshIds.push(ws.meshId);
  });

  console.log(`Room ${roomId} launching with ${matchedPlayers.length} players!`);

  for (const ws of matchedPlayers) {
    if (ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify({
        type: "match_found",
        role: ws.isHost ? "host" : "client",
        meshId: ws.meshId,
        peers: meshIds.filter((id) => id !== ws.meshId),
        peerId: ws.peerId,
        roomId: roomId
      }));
    }
  }

  if (waitingQueue.length > 0) {
    _evaluateQueueState();
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

  _broadcastRoomStandings(room);
}

function _broadcastRoomStandings(room) {
  const standings = Array.from(room.results.values());

  standings.sort((a, b) => {
    if (a.finished && b.finished) return a.time - b.time;
    if (a.finished !== b.finished) return a.finished ? -1 : 1;
    return b.score - a.score;
  });

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
