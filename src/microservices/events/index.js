const express = require('express');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8082;
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'kafka:9092').split(',');

const kafka = new Kafka({
  clientId: 'events-service',
  brokers: KAFKA_BROKERS,
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'events-service-group' });

async function startKafka() {
  await producer.connect();
  await consumer.connect();

  await consumer.subscribe({ topic: 'movie-events', fromBeginning: true });
  await consumer.subscribe({ topic: 'user-events', fromBeginning: true });
  await consumer.subscribe({ topic: 'payment-events', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const value = message.value ? message.value.toString() : null;
        console.log(`Consumed message from ${topic}:`, value);
      } catch (err) {
        console.error('Error processing message', err);
      }
    },
  });
}

startKafka().catch((err) => {
  console.error('Kafka start error', err);
});

async function publish(topic, payload) {
  try {
    const msg = { value: JSON.stringify(payload) };
    await producer.send({ topic, messages: [msg] });
    console.log(`Produced to ${topic}:`, payload);
  } catch (err) {
    console.error('Produce error', err);
    throw err;
  }
}

app.get('/api/events/health', (req, res) => res.json({ status: true }));

app.post('/api/events/movie', async (req, res) => {
  const payload = { type: 'movie', data: req.body };
  try {
    await publish('movie-events', payload);
    return res.status(201).json({ status: 'success' });
  } catch (err) {
    return res.status(500).json({ status: 'error', detail: String(err) });
  }
});

app.post('/api/events/user', async (req, res) => {
  const payload = { type: 'user', data: req.body };
  try {
    await publish('user-events', payload);
    return res.status(201).json({ status: 'success' });
  } catch (err) {
    return res.status(500).json({ status: 'error', detail: String(err) });
  }
});

app.post('/api/events/payment', async (req, res) => {
  const payload = { type: 'payment', data: req.body };
  try {
    await publish('payment-events', payload);
    return res.status(201).json({ status: 'success' });
  } catch (err) {
    return res.status(500).json({ status: 'error', detail: String(err) });
  }
});

app.listen(PORT, () => console.log('events service started on port', PORT));
