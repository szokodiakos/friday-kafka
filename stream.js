const { KafkaStreams } = require("kafka-streams");
const { kafkaHost, topic } = require("./config");

const factory = new KafkaStreams({
  noptions: {
    "group.id": "hello-world-3",
    "metadata.broker.list": kafkaHost
  }
});

const kstream = factory.getKStream(topic);

kstream
  .map(msg => ({ key: msg.value.toString(), value: undefined }))
  .countByKey("key", "count")
  .map(({ key, count }) => `${key} ${count}`)
  .to("word-count-topic-lul");

kstream.start().then(
  () => {
    console.log("stream is a go");
  },
  err => {
    console.log("stream is not a go", err);
  }
);

process.on("SIGINT", function() {
  kstream.close().then(() => {
    process.exit();
  });
});
