import { Kafka, logLevel } from 'kafkajs';
import express from 'express';
import cors from 'cors';

console.log('Starting backend service...');

// ---------------- Config ----------------
const BROKERS = (process.env.KAFKA_BROKERS || 'kafka:9092')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

const TOPICS = (process.env.KAFKA_TOPICS || 'energy_data,meterological_observations,processed_data')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

const PORT = parseInt(process.env.PORT || '3000', 10);
const MAX_MESSAGES_PER_METRIC = parseInt(process.env.MAX_MESSAGES_PER_METRIC || '3', 10);

console.log({ BROKERS, TOPICS, PORT, MAX_MESSAGES_PER_METRIC });

// ---------------- Kafka ----------------
const kafka = new Kafka({
  clientId: 'demo-backend',
  brokers: BROKERS,
  logLevel: logLevel.INFO,
});

const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID || 'demo-consumer-group' });

// ---------------- Message Store ----------------
// topic -> metric -> array of last N messages
const messageStore = new Map();

// Helper to push per-metric limited messages
function pushMetric(topic, metric, message) {
  if (!messageStore.has(topic)) messageStore.set(topic, new Map());
  const topicMap = messageStore.get(topic);

  if (!topicMap.has(metric)) topicMap.set(metric, []);
  const arr = topicMap.get(metric);

  arr.push(message);
  if (arr.length > MAX_MESSAGES_PER_METRIC) arr.shift(); // remove oldest
}

// ---------------- Express ----------------
const app = express();
app.use(cors());

app.get('/health', (req, res) => res.json({ ok: true }));

// Get all topics with latest metrics
app.get('/data', (req, res) => {
  const result = {};
  for (const [topic, metrics] of messageStore.entries()) {
    result[topic] = {};
    for (const [metric, arr] of metrics.entries()) {
      result[topic][metric] = arr[arr.length - 1] || null; // latest value
    }
  }
  res.json(result);
});

// Get a specific metric in a topic
app.get('/data/:topic/:metric', (req, res) => {
  const { topic, metric } = req.params;
  if (!messageStore.has(topic)) return res.status(404).json({ error: 'Unknown topic' });
  const metrics = messageStore.get(topic);
  if (!metrics.has(metric)) return res.status(404).json({ error: 'Unknown metric' });

  res.json(metrics.get(metric));
});

// Start HTTP server
app.listen(PORT, () => console.log(`HTTP server running on :${PORT}`));

// ---------------- Kafka Consumer ----------------
const parseMessage = (message) => {
  try {
    return JSON.parse(message.value.toString());
  } catch {
    return message.value.toString();
  }
};

const runConsumer = async () => {
  await consumer.connect();
  console.log('Kafka consumer connected');

  for (const topic of TOPICS) {
    await consumer.subscribe({ topic, fromBeginning: false });
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const parsed = parseMessage(message);

      // Store each property as a separate metric
      if (parsed?.properties) {
        for (const [metric, value] of Object.entries(parsed.properties)) {
          const serialized = {
            timestamp: Number(message.timestamp),
            partition,
            offset: message.offset,
            stationId: parsed.properties.stationId,
            value,
            observed: parsed.properties.observed,
            coordinates: parsed.geometry?.coordinates || null,
          };
          pushMetric(topic, metric, serialized);
        }
      }
    },
  });
};

// Start Kafka consumer
runConsumer().catch(err => {
  console.error('Kafka consumer error:', err);
  process.exit(1);
});

// ---------------- Graceful shutdown ----------------
const shutdown = async () => {
  console.log('Shutting down...');
  await consumer.disconnect();
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
