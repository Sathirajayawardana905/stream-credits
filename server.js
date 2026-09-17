const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const fs = require('fs');
const axios = require('axios');
const { WebcastPushConnection } = require('tiktok-live-connector');

// ================= CONFIGURATION =================
const TIKTOK_USERNAME = "YOUR_TIKTOK_USERNAME"; // Without '@' (Leave empty or unchanged if not using TikTok)
const YT_API_KEY = "YOUR_YOUTUBE_API_KEY";       // From Google Cloud Console
const YT_VIDEO_ID = "YOUR_YOUTUBE_VIDEO_ID";     // The live stream ID from the URL (watch?v=XXXX)
const PORT = process.env.PORT || 8080;
// =================================================

const app = express();
const server = http.createServer(app);

// Serve credits.html and static files to browser or OBS
app.use(express.static(__dirname));

const wss = new WebSocketServer({ server });

// Ensure stream history folder exists
const historyDir = path.join(__dirname, 'stream_history');
if (!fs.existsSync(historyDir)) {
  fs.mkdirSync(historyDir);
}

// Global session state
let currentSession = {
  streamDate: new Date().toISOString(),
  subscribers: new Set(),
  gifters: {},
  topLikers: {},
  superChats: {}
};

function getCleanPayload() {
  return {
    subscribers: Array.from(currentSession.subscribers),
    gifters: currentSession.gifters,
    topLikers: currentSession.topLikers,
    superChats: currentSession.superChats
  };
}

function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === 1) client.send(msg);
  });
}

// WebSocket connections from OBS / Browser Source
wss.on('connection', ws => {
  ws.send(JSON.stringify({ type: 'CREDITS_DATA', payload: getCleanPayload() }));

  ws.on('message', message => {
    try {
      const data = JSON.parse(message);
      if (data.action === 'NEW_STREAM') {
        // 1. Archive current stream to file
        const fileName = `stream_${new Date().toISOString().slice(0, 10)}_${Date.now()}.json`;
        fs.writeFileSync(path.join(historyDir, fileName), JSON.stringify(getCleanPayload(), null, 2));

        // 2. Reset session data
        currentSession = {
          streamDate: new Date().toISOString(),
          subscribers: new Set(),
          gifters: {},
          topLikers: {},
          superChats: {}
        };

        console.log(`[SYSTEM] New stream session started. Archived to ${fileName}`);
        broadcast({ type: 'CREDITS_DATA', payload: getCleanPayload() });
      }
    } catch (err) {
      console.error("[WS] Message parsing error:", err.message);
    }
  });
});

// ---------------- 1. TIKTOK INTEGRATION ----------------
if (TIKTOK_USERNAME && TIKTOK_USERNAME !== "YOUR_TIKTOK_USERNAME") {
  const tiktok = new WebcastPushConnection(TIKTOK_USERNAME);

  tiktok.connect()
    .then(state => console.log(`[TikTok] Connected to LIVE room ID: ${state.roomId}`))
    .catch(err => console.log(`[TikTok] Waiting for stream to go live (${err.message})`));

  tiktok.on('subscribe', data => {
    currentSession.subscribers.add(data.uniqueId);
    console.log(`[TikTok Sub] ${data.uniqueId}`);
  });

  tiktok.on('gift', data => {
    const user = data.uniqueId;
    const count = data.repeatCount || 1;
    currentSession.gifters[user] = (currentSession.gifters[user] || 0) + count;
    console.log(`[TikTok Gift] ${user} sent ${data.giftName} x${count}`);
  });

  tiktok.on('like', data => {
    const user = data.uniqueId;
    currentSession.topLikers[user] = (currentSession.topLikers[user] || 0) + data.likeCount;
  });
}

// ---------------- 2. YOUTUBE INTEGRATION ----------------
let nextPollPageToken = null;
let ytPollingInterval = 6000;

async function pollYouTubeLive() {
  if (!YT_API_KEY || YT_API_KEY.includes("YOUR_") || !YT_VIDEO_ID || YT_VIDEO_ID.includes("YOUR_")) {
    return;
  }

  try {
    // Fetch liveChatId
    const streamRes = await axios.get(`https://www.googleapis.com/youtube/v3/videos`, {
      params: {
        part: 'liveStreamingDetails',
        id: YT_VIDEO_ID,
        key: YT_API_KEY
      }
    });

    const liveChatId = streamRes.data.items[0]?.liveStreamingDetails?.activeLiveChatId;
    if (!liveChatId) {
      console.log("[YouTube] Stream is not active or has no live chat.");
      return;
    }

    const chatParams = {
      liveChatId: liveChatId,
      part: 'snippet,authorDetails',
      key: YT_API_KEY
    };
    if (nextPollPageToken) chatParams.pageToken = nextPollPageToken;

    const chatRes = await axios.get(`https://www.googleapis.com/youtube/v3/liveChat/messages`, { params: chatParams });

    nextPollPageToken = chatRes.data.nextPageToken;
    ytPollingInterval = chatRes.data.pollingIntervalMillis || 6000;

    chatRes.data.items.forEach(item => {
      const author = item.authorDetails.displayName;
      const snippet = item.snippet;

      if (snippet.type === 'superChatEvent') {
        const amount = snippet.superChatDetails.amountDisplayString;
        currentSession.superChats[author] = amount;
        console.log(`[YouTube SuperChat] ${author} sent${amount}`);
      }

      if (snippet.type === 'newSponsorEvent') {
        currentSession.subscribers.add(`(YT) ${author}`);
        console.log(`[YouTube Sponsor] ${author}`);
      }
    });
  } catch (err) {
    console.error("[YouTube Error]", err.response?.data?.error?.message || err.message);
  } finally {
    setTimeout(pollYouTubeLive, ytPollingInterval);
  }
}

if (YT_VIDEO_ID && !YT_VIDEO_ID.includes("YOUR_")) {
  pollYouTubeLive();
}

server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});