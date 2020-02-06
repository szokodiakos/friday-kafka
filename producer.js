const { HighLevelProducer, KafkaClient } = require("kafka-node");
const { kafkaHost, topic } = require("./config");

console.log("starting producer...");

const name = process.env.CHAT_USERNAME || "anon";

const client = new KafkaClient({ kafkaHost });
const producer = new HighLevelProducer(client);

producer.on("ready", () => {
  console.log("ready to produce");

  producer.createTopics([topic], false, (err, data) => {
    if (err) {
      console.log("error creating topic", err);
    }
  });
});
producer.on("error", err => console.log("producer error", err));

process.stdin.setEncoding("utf8");
process.stdin.on("data", message => {
  producer.send(
    [
      {
        topic,
        messages: JSON.stringify({ name, message })
      }
    ],
    err => {
      if (err) {
        console.log("error sending", err);
      }
    }
  );
});
