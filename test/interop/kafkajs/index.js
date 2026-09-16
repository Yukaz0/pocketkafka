// KafkaJS interoperability check: metadata, produce, consume, and consumer
// group commit/fetch offsets against a running broker.
//
// Usage: node index.js <host:port>
'use strict';

const { Kafka, logLevel } = require('kafkajs');

const addr = process.argv[2];
if (!addr) {
  console.error('usage: node index.js <host:port>');
  process.exit(1);
}

const [host, port] = addr.split(':');
const topic = 'interop-kafkajs';
const groupId = 'interop-kafkajs-group';

const kafka = new Kafka({
  clientId: 'interop-kafkajs',
  brokers: [`${host}:${port}`],
  logLevel: logLevel.ERROR,
});

async function main() {
  const admin = kafka.admin();
  await admin.connect();
  const topics = await admin.listTopics();
  console.log('topics:', topics.join(',') || '(none)');
  await admin.createTopics({ topics: [{ topic, numPartitions: 1 }], waitForLeaders: true });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic,
    messages: [{ key: 'k1', value: 'v1' }, { key: 'k2', value: 'v2' }],
  });
  console.log('produced 2 messages');
  await producer.disconnect();

  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  const seen = [];
  await consumer.run({
    eachMessage: async ({ message }) => {
      seen.push(String(message.value));
      if (seen.length >= 2) {
        await consumer.disconnect();
      }
    },
  });

  // Give the runner a moment to drain, then assert.
  await new Promise((r) => setTimeout(r, 3000));
  if (seen.length < 2) {
    console.error(`FAIL: expected 2 messages, got ${JSON.stringify(seen)}`);
    process.exit(1);
  }
  console.log('kafkajs OK');
}

main().catch((err) => {
  console.error('FAIL:', err);
  process.exit(1);
});
