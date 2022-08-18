const { Kafka } = require('kafkajs');
const axios = require('axios');
const path = require('path');

class Connector {
  constructor(app, config) {
    this.kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: [config.kafka.host],
    });
    this.kafkaProducer = this.kafka.producer();
    this.app = app;
    this.config = config;
  }

  async produce(topic, key, value) {
    const reformattedValue = { key, value };
    await this.kafkaProducer.connect();
    await this.kafkaProducer
      .send({
        topic,
        messages: [{ value: JSON.stringify(reformattedValue) }],
      })
      .catch((err) => {
        throw err;
      });

    await this.kafkaProducer.disconnect();
  }

  async consume(groupId, topic, callback) {
    const consumer = this.kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ message }) => {
        callback(JSON.parse(message.value.toString()));
      },
    });
  }

  async produceSync(service, key, value) {
    const serviceHost = this.config.services[service].host;

    // eslint-disable-next-line no-return-await
    return axios.post(path.join(serviceHost, key), value);
  }
}

module.exports = Connector;
