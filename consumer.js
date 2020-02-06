const { ConsumerGroup } = require("kafka-node");
const { consumerGroupId, kafkaHost, topic } = require("./config");

console.log("starting consumer...");

const consumer = new ConsumerGroup(
  {
    groupId: consumerGroupId,
    kafkaHost,
    fromOffset: "earliest",
    onRebalance: (isAlreadyMember, callback) => {
      console.log("rebalance happened", isAlreadyMember);
      callback();
    }
  },
  topic
);

consumer.on("connect", () => console.log("ready to consume"));

consumer.on("error", err => {
  console.log("consumer error", err);
});

consumer.on("message", json => {
  const { name, message } = JSON.parse(json.value);
  console.log(`${name} says: ${message}`);
});
