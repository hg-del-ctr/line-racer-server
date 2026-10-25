const { WebSocketServer } = require("ws");
const Database = require("better-sqlite3");
const path = require("path");

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// --- SQLite Database Initialization ---
const dbPath = path.join(__dirname, "leaderboard.db");
const db = new Database(dbPath);

// Enable WAL mode for high concurrency
db.pragma("journal_mode = WAL");

db.exec(`
  CREATE TABLE IF NOT EXISTS players (
    uuid TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    skin_id TEXT DEFAULT 'ball_red_small',
    wins INTEGER DEFAULT 0,
    updated_at INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_players_wins ON players(wins DESC);
`);

const upsertPlayerStmt = db.prepare(`
  INSERT INTO players (uuid, name, skin_id, wins, updated_at)
  VALUES (@uuid, @name, @skin_id, 0, @updated_at)
  ON CONFLICT(uuid) DO UPDATE SET
    name = excluded.name,
    skin_id = excluded.skin_id,
    updated_at = excluded.updated_at
`);

const recordWinStmt = db.prepare(`
  INSERT INTO players (uuid, name, skin_id, wins, updated_at)
  VALUES (@uuid, @name, @skin_id, 1, @updated_at)
  ON CONFLICT(uuid) DO UPDATE SET
    name = excluded.name,
    skin_id = excluded.skin_id,
    wins = players.wins + 1,
    updated_at = excluded.updated_at
`);

const getTopLeaderboardStmt = db.prepare(`
  SELECT uuid, name, skin_id, wins,
         RANK() OVER (ORDER BY wins DESC, updated_at ASC) as rank
  FROM players
  WHERE wins > 0
  ORDER BY wins DESC, updated_at ASC
  LIMIT 50
`);

const getPlayerRankStmt = db.prepare(`
  WITH ranked_players AS (
    SELECT uuid, wins,
           RANK() OVER (ORDER BY wins DESC, updated_at ASC) as rank
    FROM players
    WHERE wins > 0
  )
  SELECT rank, wins FROM ranked_players WHERE uuid = ?
`);

const getPlayerRawWinsStmt = db.prepare(`
  SELECT wins FROM players WHERE uuid = ?
`);

function _dbGetPlayerStats(uuid) {
  if (!uuid) return { rank: null, wins: 0 };
  const ranked = getPlayerRankStmt.get(uuid);
  if (ranked) {
    return { rank: ranked.rank, wins: ranked.wins };
  }
  const raw = getPlayerRawWinsStmt.get(uuid);
  return { rank: null, wins: raw ? raw.wins : 0 };
}

function _dbRecordPlayerWin(uuid, name, skinId) {
  if (!uuid) return { rank: null, wins: 0 };
  const now = Date.now();
  recordWinStmt.run({
    uuid: uuid,
    name: name,
    skin_id: skinId || "ball_red_small",
    updated_at: now
  });
  return _dbGetPlayerStats(uuid);
}

// --- Server & Matchmaking State ---
let waitingQueue = [];
let queueTimer = null;
let queueCountdown = 0;

let nextPeerId = 1;
let nextRoomId = 1;

const rooms = new Map();
let onlineCountBroadcastTimeout = null;

console.log(`Matchmaking, Signaling & Leaderboard server running on port ${PORT}`);

// Broadcast online counter with 300ms debounce
function _scheduleOnlineCountBroadcast() {
  if (onlineCountBroadcastTimeout) return;
  onlineCountBroadcastTimeout = setTimeout(() => {
    onlineCountBroadcastTimeout = null;
    _broadcastOnlineCount();
  }, 300);
}

function _broadcastOnlineCount() {
  const count = wss.clients.size;
  const payload = JSON.stringify({
    type: "online_count",
    count: count
  });

  for (const client of wss.clients) {
    if (client.readyState === client.OPEN) {
      client.send(payload);
    }
  }
}

wss.on("connection", (ws) => {
  ws.peerId = nextPeerId++;
  ws.playerUuid = null;
  ws.room = null;
  ws.meshId = 0; // 1 = Host, 2..4 = Clients
  ws.playerName = `Player ${ws.peerId}`;
  ws.skinId = "ball_red_small";
  ws.globalRank = null;
  ws.wins = 0;

  console.log(`Player connected. Assigned Server ID: ${ws.peerId} (Total Online: ${wss.clients.size})`);

  // Send immediate online count to the newcomer
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify({
      type: "online_count",
      count: wss.clients.size
    }));
  }
  _scheduleOnlineCountBroadcast();

  ws.on("message", (rawMessage) => {
    let msg;
    try {
      msg = JSON.parse(rawMessage);
    } catch (e) {
      return;
    }

    switch (msg.type) {
      case "queue":
        if (msg.uuid) ws.playerUuid = String(msg.uuid);
        if (msg.name) ws.playerName = String(msg.name);
        if (msg.skin_id) ws.skinId = String(msg.skin_id);

        if (ws.playerUuid) {
          upsertPlayerStmt.run({
            uuid: ws.playerUuid,
            name: ws.playerName,
            skin_id: ws.skinId,
            updated_at: Date.now()
          });
          const stats = _dbGetPlayerStats(ws.playerUuid);
          ws.globalRank = stats.rank;
          ws.wins = stats.wins;
        }

        _handleQueue(ws);
        break;

      case "get_leaderboard": {
        const top50 = getTopLeaderboardStmt.all();
        const requesterUuid = msg.uuid || ws.playerUuid;
        const requesterStats = _dbGetPlayerStats(requesterUuid);

        if (ws.readyState === ws.OPEN) {
          ws.send(JSON.stringify({
            type: "leaderboard_data",
            top_players: top50,
            player_stats: {
              uuid: requesterUuid,
              rank: requesterStats.rank,
              wins: requesterStats.wins
            }
          }));
        }
        break;
      }

      case "signal":
        // Route WebRTC offer/answer/ICE candidates to target peer
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
    console.log(`Player disconnected: ${ws.peerId} (Remaining Online: ${wss.clients.size - 1})`);
    _scheduleOnlineCountBroadcast();

    // 1. If in matchmaking queue
    const wasInQueue = waitingQueue.includes(ws);
    waitingQueue = waitingQueue.filter((p) => p !== ws);
    if (wasInQueue) {
      _evaluateQueueState();
    }

    // 2. If in active race room
    const room = ws.room;
    if (room) {
      if (!room.results.has(ws.peerId)) {
        room.results.set(ws.peerId, {
          id: ws.peerId,
          uuid: ws.playerUuid,
          name: ws.playerName,
          skin_id: ws.skinId,
          global_rank: ws.globalRank,
          wins: ws.wins,
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
  console.log(`Player ${ws.peerId} (${ws.playerName}, UUID: ${ws.playerUuid}, Rank: #${ws.globalRank ?? "Unranked"}) joined queue. Total waiting: ${waitingQueue.length}`);

  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify({
      type: "queued",
      peerId: ws.peerId,
      online_count: wss.clients.size
    }));
  }

  _evaluateQueueState();
}

function _evaluateQueueState() {
  waitingQueue = waitingQueue.filter((p) => p.readyState === p.OPEN);

  if (waitingQueue.length >= 4) {
    if (queueTimer) {
      clearInterval(queueTimer);
      queueTimer = null;
    }
    _launchMatchFromQueue();
    return;
  }

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
    uuid: ws.playerUuid,
    name: ws.playerName,
    skin_id: ws.skinId || "ball_red_small",
    global_rank: ws.globalRank,
    wins: ws.wins
  }));

  const payload = JSON.stringify({
    type: "queue_status",
    count: waitingQueue.length,
    online_count: wss.clients.size,
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
    ws.meshId = idx + 1; // 1 = Host, 2..4 = Clients
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

    // 1st place in multiplayer gets recorded in the global leaderboard
    if (assignedRank === 1 && ws.playerUuid) {
      const updated = _dbRecordPlayerWin(ws.playerUuid, playerName, ws.skinId);
      ws.wins = updated.wins;
      ws.globalRank = updated.rank;
      console.log(`LEADERBOARD: 🏆 Winner ${playerName} (${ws.playerUuid}) earned win #${ws.wins}! Global Rank: #${ws.globalRank}`);
    }

    if (ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify({
        type: "placement_announced",
        rank: assignedRank,
        global_rank: ws.globalRank,
        wins: ws.wins
      }));
    }
  }

  room.results.set(ws.peerId, {
    id: ws.peerId,
    uuid: ws.playerUuid,
    name: playerName,
    skin_id: ws.skinId,
    global_rank: ws.globalRank,
    wins: ws.wins,
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
        uuid: playerWs.playerUuid,
        name: playerWs.playerName,
        skin_id: playerWs.skinId,
        global_rank: playerWs.globalRank,
        wins: playerWs.wins,
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
