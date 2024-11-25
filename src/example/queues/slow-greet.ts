import { IQueueProcess } from "../../lib";
import { sleep } from "../utils";

export const slowGreet: IQueueProcess<{ name: string }> = {
  name: "slow-greet",
  process: async ({ data }) => {
    console.info("@slow-greet ", "started");
    await sleep(8000);
    console.info("@slow-greet", `Hello ${data.name}`);
    console.info("@slow-greet ", "completed");
  },
};