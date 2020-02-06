const { ConsumerGroup } = require("kafka-node");
const { consumerGroupId, kafkaHost, streamsTopic } = require("./config");

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
  streamsTopic
);

consumer.on("connect", () => console.log("ready to consume"));

consumer.on("error", err => {
  console.log("consumer error", err);
});

consumer.on("message", message => {
  console.log(message.value);
});
