import { IQueueProcess } from "../../lib";

export const groupGreet: IQueueProcess<{ name: string }> = {
  name: "group-greet",
  action: async ({ data }) => {
    console.info(`Hey `, data.name, " Geeting from app2");
  },
  groupName: "app2",
};
