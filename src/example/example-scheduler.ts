import { IQueueProcess, QueueManager } from "../lib";
import { sleep } from "./utils";

const ping: IQueueProcess<{ name: string }> = {
  name: "ping",
  process: async () => {
    console.log("Ping");
  },
};

const queues = new QueueManager(
  {
    ping,
  },
  {
    connection: {
      host: "localhost",
      port: 6379,
    },
    namespace: "my-schedules",
  }
);

async function start() {
  return new Promise(async (resolve) => {
    await queues.startQueues();
    console.log("started queue");

    await sleep(1000);

    let job = await queues.scheduleJob(
      "ping",
      "pingme",
      { every: 2000 },
      { name: "my-ping", data: { name: "Pong" } }
    );
    console.log(`Job added `, job?.id);

    await queues.startWorkers();
    console.log("started worker");

    setTimeout(async () => {
      await queues.removeScheduleJob("ping", "pingme");
    }, 5000);

    setTimeout(async () => {
      await queues.shutdown();
      resolve("ok");
    }, 10000);
  });
}

start().then(() => {
  console.log("Example schedule completed");
});
