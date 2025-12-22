import { Kafka, logLevel } from 'kafkajs';
import express from 'express';
import cors from 'cors';


console.log('Starting backend service...');

// Read brokers and topics from env; fall back to sane defaults
const BROKERS = (process.env.KAFKA_BROKERS || 'kafka:9092')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const TOPICS = (process.env.KAFKA_TOPICS || 'energy_data,meterological_observations,processed_data')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const PORT = parseInt(process.env.PORT || '3000', 10);

const kafka = new Kafka({
  clientId: 'node-consumer',
  brokers: BROKERS,
  logLevel: logLevel.INFO,
});

// Minimal HTTP server for k8s probes and quick checks
const app = express();
app.use(cors());
app.get('/health', (req, res) => res.status(200).json({ ok: true }));
app.get('/info', (req, res) => res.json({ brokers: BROKERS, topics: TOPICS }));
app.listen(PORT, () => console.log(`HTTP listening on :${PORT}`, { brokers: BROKERS, topics: TOPICS }));

app.get('/data', (req, res) => {
  try {
    const summary = {};
    for (const [topic, messages] of messageStore.entries()) {
      summary[topic] = { count: messages.length, latest: messages[messages.length - 1] };
    }
    res.json(summary);
  } catch (err) {
    console.error('Error in /data:', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/data/:topic', (req, res) => {
  try {
    const { topic } = req.params;
    const limit = Math.min(parseInt(req.query.limit || '50', 10), 1000);

    if (!messageStore.has(topic)) {
      return res.status(404).json({ error: 'Unknown topic' });
    }

    const messages = messageStore.get(topic);
    const slice = messages.slice(-limit);
    res.json({ topic, count: slice.length, total: messages.length, messages: slice });
  } catch (err) {
    console.error(`Error in /data/${req.params.topic}:`, err);
    res.status(500).json({ error: err.message });
  }
});

const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID || 'node-consumer-group' });

// Utility to parse messages safely
const parseMessage = (message) => {
  try {
    return JSON.parse(message.value.toString());
  } catch {
    return message.value.toString();
  }
};

// How many messages to keep per topic (fallback if no time limit)
const MAX_MESSAGES_PER_TOPIC = parseInt(
  process.env.MAX_MESSAGES_PER_TOPIC || '1000',
  10
);

// Time limits per topic (in milliseconds)
const TIME_LIMITS = {
  'meterological_observations': 2 * 24 * 60 * 60 * 1000, // 2 days
  'energy_data': 7 * 24 * 60 * 60 * 1000, // 7 days
  'processed_data': 7 * 24 * 60 * 60 * 1000, // 7 days
};

// topic -> array of serialized messages
const messageStore = new Map();
const lastCleanup = new Map(); // Track last cleanup time per topic

const pushLimited = (topic, message) => {
  if (!messageStore.has(topic)) {
    messageStore.set(topic, []);
    lastCleanup.set(topic, Date.now());
  }

  const arr = messageStore.get(topic);
  arr.push(message);

  // Time-based cleanup if configured (run every 5 minutes max)
  const timeLimit = TIME_LIMITS[topic];
  const now = Date.now();
  const lastClean = lastCleanup.get(topic) || 0;
  const cleanupInterval = 5 * 60 * 1000; // 5 minutes

  if (timeLimit && (now - lastClean) > cleanupInterval) {
    const cutoff = now - timeLimit;
    const originalLength = arr.length;
    
    // Find first index that's still valid (binary search would be better for huge arrays)
    let firstValid = 0;
    while (firstValid < arr.length && arr[firstValid].timestamp < cutoff) {
      firstValid++;
    }
    
    if (firstValid > 0) {
      // Remove old messages in-place
      arr.splice(0, firstValid);
      console.log(`[${topic}] Purged ${firstValid} old messages, ${arr.length} remain`);
    }
    
    lastCleanup.set(topic, now);
  } else if (!timeLimit && arr.length > MAX_MESSAGES_PER_TOPIC) {
    // Count-based limit as fallback
    arr.shift(); // remove oldest
  }
};

// Connect and subscribe before starting the consumer loop
await consumer.connect();
console.log('Consumer connected', { brokers: BROKERS, topics: TOPICS });
for (const topic of TOPICS) {
  await consumer.subscribe({ topic, fromBeginning: false });
}

await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    const parsed = parseMessage(message);

    // Serialize / normalize
    const serialized = {
      topic,
      partition,
      offset: message.offset,
      timestamp: Number(message.timestamp),

      stationId: parsed?.properties?.stationId,
      parameterId: parsed?.properties?.parameterId,
      value: parsed?.properties?.value,
      observed: parsed?.properties?.observed,

      coordinates: parsed?.geometry?.coordinates,
    };

    pushLimited(topic, serialized);

    // Debug: log store size periodically (every 50 messages now)
    const arr = messageStore.get(topic);
    if (arr && arr.length % 50 === 0) {
      const oldest = arr[0]?.timestamp;
      const ageHours = oldest ? ((Date.now() - oldest) / (1000 * 60 * 60)).toFixed(1) : 'N/A';
      console.log(`[${topic}] Store: ${arr.length} messages, oldest: ${ageHours}h ago`);
    }

    // Optional: cleaner logging (reduced verbosity)
    if (Math.random() < 0.05) { // log 5% of messages
      console.log(
        `[${topic}] ${serialized.parameterId} = ${serialized.value}`
      );
    }
  },
});

// Optional: graceful shutdown
const shutdown = async () => {
  try {
    await consumer.disconnect();
  } finally {
    process.exit(0);
  }
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
