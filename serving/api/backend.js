import { Kafka, logLevel } from 'kafkajs';
import express from 'express';
import cors from 'cors';

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

const consumer = kafka.consumer({ groupId: process.env.KAFKA_GROUP_ID || 'node-consumer-group' });

// Minimal HTTP server for k8s probes and quick checks
const app = express();
app.use(cors());
app.get('/health', (req, res) => res.status(200).json({ ok: true }));
app.get('/info', (req, res) => res.json({ brokers: BROKERS, topics: TOPICS }));
app.listen(PORT, () => console.log(`HTTP listening on :${PORT}`, { brokers: BROKERS, topics: TOPICS }));

// Utility to parse messages safely
const parseMessage = (message) => {
  try {
    return JSON.parse(message.value.toString());
  } catch {
    return message.value.toString();
  }
};

const run = async () => {
  await consumer.connect();
  console.log('Consumer connected');
  console.log('Using brokers:', BROKERS);

  // Subscribe to multiple topics
  for (const topic of TOPICS) {
    await consumer.subscribe({ topic, fromBeginning: true });
  }

  // Consume messages
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const parsed = parseMessage(message);
      console.log(`[${topic} | partition ${partition} | offset ${message.offset}]`, parsed);
    },
  });
};



run().catch((err) => {
  console.error('Error in consumer:', err);
  process.exit(1);
});
