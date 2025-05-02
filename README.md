# @srttk/queue 🎡

A simplified wrapper around BullMQ for easy queue management in Node.js applications. provides a straightforward way to define, manage, and process queues with minimal boilerplate.

## Features

- Simple queue definition interface
- Type-safe queue processing
- Easy worker management
- Simplified job addition API
- Built on top of robust BullMQ infrastructure

## Installation

```bash
npm install @srttk/queue bullmq
```

## Basic Usage

### 1. Define Your Queue

Create a queue processor by defining a type for your payload and implementing the `IQueueProcess` interface:

```typescript
// greet.ts
import { IQueueProcess } from "@srttk/queue"

type GreetPayload = {
  name: string
}

type Result {
  message: string
}

export const greet: IQueueProcess<GreetPayload, Result> = {
  name: "greet",
  process: async ({ data }) => {

    const message = `Hello ${data.name}`;
    console.log(message)

    // Return result - optional
    return { messsage }
  }
   onCompleted: (job, result) => {
   // Job completed
   console.info(`Job ${job.id} completed `, result)
  },
  onFailed: async (job) => {
    // Job Failed
    console.error(`Job $job.id} failed.`)
  },
}
```

### 2. Create QueueManager Instance

Set up your queue manager with your defined queues:

```typescript
// queue.ts
import { QueueManager } from "@srttk/queue"
import { greet } from './greet'

export default new QueueManager({ greet })
```

### 3. Initialize and Use Queues

In your main application entry point:

```typescript
// app.ts
import queue from "./queue"

async function start() {
  // Initialize queue manager
  await queue.startQueues();
  await queue.startWorkers();

  // Add job to queue
  await queue.addJob("greet", "my-job", { name: "Luke Skywalker" });
}



start()
```


## Lifecycle Events

The queue system provides comprehensive event handling through various lifecycle hooks. These events allow you to monitor and respond to different states of your jobs and queues.

### Job Events

#### `onCompleted(job: Job, result: any)`
Triggered when a job is successfully completed.
```typescript
onCompleted: (job, result) => {
  console.log(`Job ${job.id} completed with result:`, result);
}
```

#### `onFailed(job: Job, error: Error)`
Triggered when a job fails due to an error.
```typescript
onFailed: (job, error) => {
  console.error(`Job ${job.id} failed:`, error.message);
}
```

#### `onProgress(job: Job, progress: number)`
Triggered when job progress is updated.
```typescript
onProgress: (job, progress) => {
  console.log(`Job ${job.id} is ${progress}% complete`);
}
```

#### `onActive(job: Job)`
Triggered when a job starts processing.
```typescript
onActive: (job) => {
  console.log(`Job ${job.id} has started processing`);
}
```

#### `onStalled(job: Job)`
Triggered when a job is stalled (worker is not responding).
```typescript
onStalled: (job) => {
  console.warn(`Job ${job.id} has stalled`);
}
```

### Queue Events

#### `onReady()`
Triggered when the queue is ready to process jobs.
```typescript
onReady: () => {
  console.log('Queue is ready to process jobs');
}
```

#### `onPaused()`
Triggered when the queue is paused.
```typescript
onPaused: () => {
  console.log('Queue has been paused');
}
```



### Example with Multiple Events

```typescript
export const emailProcessor: IQueueProcess<EmailPayload> = {
  name: "email",
  process: async ({ data }) => {
    // Process email
  },
  onCompleted: (job, result) => {
    console.log(`Email sent successfully: ${job.id}`);
  },
  onFailed: (job, error) => {
    console.error(`Failed to send email: ${error.message}`);
  },
  onProgress: (job, progress) => {
    console.log(`Sending email... ${progress}%`);
  },
  onActive: (job) => {
    console.log(`Starting to send email: ${job.id}`);
  }
}
```

## Scheduled Jobs

The QueueManager provides a way to schedule recurring jobs using BullMQ's scheduler functionality.

### Scheduling Jobs

Use the `scheduleJob` method to create or update a scheduled job:

```typescript
// Schedule a job to run every day at 9:00 AM
await queue.scheduleJob(
  "greet",                   // Queue name
  "daily-greeting",          // Scheduler ID
  { 
    pattern: "0 9 * * *"     // Cron pattern
  },
  {
    name: "morning-greeting", // Job name
    data: { name: "Team" },   // Job payload
    opts: { priority: 2 }     // Optional job options
  }
);
```

### Removing Scheduled Jobs

To remove a scheduled job:

```typescript
await queue.removeScheduleJob("greet", "daily-greeting");
```

## Graceful shutdown
QueueManager provides a graceful shutdown mechanism to ensure that in-progress jobs are completed and resources are properly released when your application terminates.

```typescript

let signals = ["SIGINT", "SIGTERM"];
signals.map((signal) => {
  process.on(signal, async () => {
    // close all queuee and workers
    await queue.shutdown();
  });
});

```

## Namespace
Namespaces allow you to isolate and group your queues, preventing naming conflicts across different applications or environments. By using namespaces, you can
 - Separate queues for different applications
 - Create isolated environments (development, staging, production)
 - Avoid queue name collisions in shared infrastructure

```typescript

export default new QueueManager({ greet }, { namespace:"app1_development"})

```

## Connection (Redis Connection)

QueueManager accepts Redis connection settings as part of its initialization options. These settings follow the same format as BullMQ connection options.

### Basic Connection Configuration

```typescript
export default new QueueManager(
  { greet},
  {
    connection: {
      host: "localhost",
      port: 6379,
    }
  }
);
```

### Configuration Options

- **connection**: Redis connection settings
  - `host`: Redis server hostname (default: "localhost")
  - `port`: Redis server port (default: 6379)
  - Additional BullMQ connection options are supported




## Worker Groups

The Queue Manager supports organizing workers into logical groups, allowing you to selectively start specific sets of workers. This is particularly useful when your application needs to run only certain workers based on different contexts or deployment scenarios.

### Defining Worker Groups

You can assign workers to groups by specifying a `groupName` in the queue process definition:

```typescript
export const greet: IQueueProcess<GreetPayload, Result> = {
  name: "greet",
  process: async ({ data }) => {
    const message = `Hello ${data.name}`;
    console.log(message);
    return {
      message,
    };
  },
  groupName: "app1"  // Assign this worker to the "app1" group
};
```

### Starting Specific Workers (Start workers by Group)

To start workers belonging to a specific group:

```typescript
const qm = new QueueManager({ greet });
qm.startQueues();
qm.startWorkers('app1');  // Only starts workers in the "app1" group
```

### Use Cases

- **Microservices**: Run different worker groups on different services
- **Development Environment**: Start only relevant workers during local development
- **Resource Optimization**: Run specific workers on dedicated servers based on their resource requirements
- **Feature Segregation**: Organize workers by feature or domain area

### Important Notes

- Workers without a `groupName` will not be started when using `startWorkers(groupName)`

- The `groupName` is optional - if not using worker groups, you can omit this property


## API Reference

### QueueManager

#### Constructor

```typescript
new QueueManager(queues: Record<string, IQueueProcess>)
```

#### Methods

- `startQueues()`: Initializes all defined queues
- `startWorkers(groupName?: string)`: Starts workers for all registered queues, optional group name
- `addJob(queueName: string, jobId: string, data: any)`: Adds a new job to the specified queue
- `getQueue(name: string)`: Get the bullmq queue instance
- `getWorker(name: string)`: Get bullmq worker instance
- `shutdown`: Close all queues and workers

### IQueueProcess Interface

```typescript
interface IQueueProcess<T> {
  name: string;
  process: (job: { data: T }) => Promise<void>;
}
```

## Configuration

The package uses BullMQ under the hood. You can configure Redis connection settings and other options when initializing the QueueManager (documentation coming soon).

## TypeScript Support

This package is written in TypeScript and includes type definitions. It provides full type safety for your queue payloads and processors.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

MIT

## Support

For issues and feature requests, please use the [GitHub issues page](https://github.com/srttk/queue/issues).