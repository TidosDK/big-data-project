import { Kafka, logLevel } from 'kafkajs';
import express from 'express';
import cors from 'cors';
import axios from 'axios';
import avro from 'avsc';
import { promises as fs } from 'fs';
import os from 'os';
import path from 'path';

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

// HDFS Configuration
const HDFS_BASE_URL = process.env.HDFS_BASE_URL || 'http://namenode:9870/webhdfs/v1';
const REPORTS_PATH = '/reports';

// Latest HDFS report
app.get('/reports/latest', async (req, res) => {
  try {
    // 1. List report directories
    const listUrl = `${HDFS_BASE_URL}${REPORTS_PATH}?op=LISTSTATUS`;
    const listRes = await axios.get(listUrl);
    const files = listRes.data.FileStatuses.FileStatus;

    if (!files || files.length === 0) {
      return res.status(404).json({ error: 'No reports found in HDFS' });
    }

    // Get the latest directory (e.g., accuracy_avro)
    const latestDir = files
      .filter(f => f.type === 'DIRECTORY')
      .sort((a, b) => b.modificationTime - a.modificationTime)[0];
    
    if (!latestDir) {
      return res.status(404).json({ error: 'No report directories found' });
    }

    const dirName = latestDir.pathSuffix;
    const dirPath = `${REPORTS_PATH}/${dirName}`;

    // 2. List Avro files inside the directory
    const filesUrl = `${HDFS_BASE_URL}${dirPath}?op=LISTSTATUS`;
    const filesRes = await axios.get(filesUrl);
    const avroFiles = filesRes.data.FileStatuses.FileStatus.filter(f => f.type === 'FILE');

    if (avroFiles.length === 0) {
      return res.status(404).json({ error: 'No Avro files in report directory' });
    }

    // 3. Read all Avro files and combine records
    const allRecords = [];
    for (const file of avroFiles) {
      // Skip empty Hadoop success flags or empty files
      if (file.length === 0 || file.pathSuffix.startsWith('_')) continue;

      const filePath = `${dirPath}/${file.pathSuffix}`;
      const readUrl = `${HDFS_BASE_URL}${filePath}?op=OPEN`;

      try {
        // Fetch binary content
        const contentRes = await axios.get(readUrl, { responseType: 'arraybuffer' });
        const buffer = Buffer.from(contentRes.data);

        // Write to a temp file and decode via avsc
        const tmpFile = path.join(os.tmpdir(), `report_${Date.now()}_${file.pathSuffix}.avro`);
        await fs.writeFile(tmpFile, buffer);

        const decoder = avro.createFileDecoder(tmpFile);
        for await (const record of decoder) {
          allRecords.push(record);
        }

        // Cleanup temp file
        await fs.unlink(tmpFile).catch(() => {});
      } catch (e) {
        console.error(`Avro decode failed for ${filePath}:`, e.message);
        // Continue with next file
      }
    }

    res.json({
      fileName: dirName,
      timestamp: latestDir.modificationTime,
      fileCount: avroFiles.length,
      data: allRecords,
    });
  } catch (err) {
    console.error('HDFS Error:', err.message);
    res.status(500).json({ error: 'Failed to retrieve HDFS report' });
  }
});

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

// List available metrics per topic
app.get('/metrics', (req, res) => {
  const result = {};
  for (const [topic, metrics] of messageStore.entries()) {
    result[topic] = Array.from(metrics.keys());
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
    await consumer.subscribe({ topic, fromBeginning: true });
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const parsed = parseMessage(message);
      //console.log(`Topic: ${topic} | Keys: ${Object.keys(parsed).join(',')}`);
      const base = {
        timestamp: Number(message.timestamp),
        partition,
        offset: message.offset,
      };

      // --- 1. Meteorological Observations (DMI Format) ---
      if (topic === 'meterological_observations' && parsed?.properties?.parameterId) {
        const metricName = parsed.properties.parameterId;
        pushMetric(topic, metricName, {
          ...base,
          stationId: parsed.properties.stationId,
          value: parsed.properties.value,
          observed: parsed.properties.observed,
          coordinates: parsed.geometry?.coordinates || null,
        });
      }

      // --- 2. Energy Data (Assuming simple key-value) ---
      if (topic === 'energy_data') {
        Object.keys(parsed).forEach(key => {
          if (key !== 'timestamp') {
            pushMetric(topic, key, { ...base, value: parsed[key] });
          }
        });
      }

      // --- 3. Processed Data (Analytics/Predictions) ---
      if (topic === 'processed_data') {
        // If it uses your specific parameterId logic:
        if (parsed?.parameterId) {
          const metricBase = parsed.parameterId;
          if (parsed.WeatherValue !== undefined) 
            pushMetric(topic, `${metricBase}_weather`, { ...base, value: parsed.WeatherValue });
          if (parsed.Actual !== undefined) 
            pushMetric(topic, `${metricBase}_actual`, { ...base, value: parsed.Actual });
          if (parsed.Predicted !== undefined) 
            pushMetric(topic, `${metricBase}_predicted`, { ...base, value: parsed.Predicted });
        } 
        // Fallback: if it's just a flat object
        else {
          Object.keys(parsed).forEach(key => {
            pushMetric(topic, key, { ...base, value: parsed[key] });
          });
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




