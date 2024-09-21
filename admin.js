const { kafka } = require("./client");

async function init() {
  const admin = kafka.admin();
  console.log("Connecting.....");
  admin.connect();
  console.log("Connection is successful.....");

  console.log("Topic Creation is started.....");
  await admin.createTopics({
    topics: [
      {
        topic: "rider-update",
        numPartitions: 2,
      },
    ],
  });
  console.log("Topic is created........");
  await admin.disconnect();
  console.log("Admin disconnected");
}

init();
