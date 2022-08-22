const { Kafka } = require('kafkajs');
const axios = require('axios');
const path = require('path');
const express = require('express');
const protectedCrossServiceRequest = require('./middlewares/protectedCrossServiceRequest');

class Connector {
  constructor(app, config, consumerActions) {
    this.kafka = new Kafka({
      clientId: config.kafka.clientId,
      brokers: [config.kafka.host],
    });
    this.kafkaProducer = this.kafka.producer();
    this.app = app;
    this.config = config;
    this.app.microservicesConfig = config;
    this.consumerActions = consumerActions;
    this.handleJSONRequests();
    this.app.use('/', this.buildServicesRoutes());
  }

  async consume(groupId, topic, callback) {
    const consumer = this.kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    consumer.on('consumer.crash', async (event) => {
      const { error } = event.payload;

      if (error && error.name !== 'KafkaJSNumberOfRetriesExceeded' && error.retriable !== true) {
        await consumer.disconnect();
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: true });
      }
    });

    await consumer.run({
      eachMessage: async ({ message }) => {
        callback(JSON.parse(message.value.toString()));
      },
    });
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

  async produceSync(topic, key, value) {
    console.log('aaaaaaaaaaaa', value)
    const serviceHost = this.config.services[topic].host;
    const headers = {
      'CROSS-SERVICE-TOKEN': this.config.crossServiceToken,
      Accept: 'application/json',
      'Content-Type': 'application/json',
    };
    const requestUrl = `http://${path.join(serviceHost, `_service/${key}`)}`;
    return axios.post(requestUrl, value, { headers });
  }

  buildServicesRoutes() {
    const router = express.Router();
    const { consumerActions, config } = this;
    if (typeof consumerActions === 'undefined' || consumerActions === null) {
      return router;
    }
    Object.keys(consumerActions).forEach(function (key) {
      router.post(`/_service/${key}`, protectedCrossServiceRequest(config), consumerActions[key]);
    });
    return router;
  }

  handleJSONRequests() {
    this.app.use(express.json());
  }
}

module.exports = Connector;
