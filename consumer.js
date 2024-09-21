const { kafka } = require("./client");
const group = process.argv[2];

async function init() {
  const consumer = kafka.consumer({ groupId: group });
  await consumer.connect();
  console.log("Consumer connected");

  await consumer.subscribe({ topics: ["rider-update"], fromBeginning: true });

  console.log("Consumer subscribed");
  await consumer.run({
    eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
      console.log(
        `${group}:[${topic}]:PART:${partition}: ${message.key.toString()}`,
        message.value.toString()
      );
    },
  });
  console.log("Consumer started running");

  //   await consumer.disconnect();
}
init();
