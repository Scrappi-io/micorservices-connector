const { Kafka } = require('kafkajs');

module.exports = (app, config) => {
  const kafka = new Kafka({
    clientId: config.kafka.clientId,
    brokers: [config.kafka.host],
  });

};
