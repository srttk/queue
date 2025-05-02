import {
  Queue,
  QueueOptions,
  Worker,
  WorkerOptions,
  ConnectionOptions,
  JobsOptions,
  RepeatOptions,
} from "bullmq";
import { IQueueProcess } from "./queue-process";

type QueueManagerConfig = {
  namespace?: string;
  connection?: ConnectionOptions;
  defaultWorkerOptions?: Partial<WorkerOptions>;
  defaultQueueOptions?: Partial<QueueOptions>;
  defaultJobOptions?: Partial<JobsOptions>;
};

const DEFAULT_NAMESPACE = "default_queues";

const DEFAULT_QUEUE_OPTIONS: Partial<QueueOptions> = {};
const DEFAULT_WORKER_OPTIONS: Partial<WorkerOptions> = {};
const DEFAULT_JOB_OPTIONS: Partial<JobsOptions> = {};

export class QueueManager<T extends Record<string, IQueueProcess>> {
  private _queueMap: T;

  private queues: Queue[] = [];
  private workers: Worker[] = [];

  private config?: QueueManagerConfig;

  private connection: ConnectionOptions;

  private shutdownTriggered: boolean = false;

  // Default options

  private _defaultQueueOptions: Partial<QueueOptions> = DEFAULT_QUEUE_OPTIONS;
  private _defaultWorkerOptions: Partial<WorkerOptions> =
    DEFAULT_WORKER_OPTIONS;
  private _defaultJobOptions: Partial<JobsOptions> = DEFAULT_JOB_OPTIONS;

  constructor(queueMaps: T, config?: QueueManagerConfig) {
    this._queueMap = queueMaps;
    this.config = config;

    if (config?.connection) {
      this.connection = config.connection;
    }
  }

  get namespace() {
    return this?.config?.namespace || DEFAULT_NAMESPACE;
  }

  startQueues() {
    if (!this._queueMap) {
      return null;
    }
    let queues = Object.values(this._queueMap);

    if (!queues?.length) {
      return null;
    }

    queues.map((q) => {
      let _options = { ...this._defaultQueueOptions, ...(q?.options || {}) };
      let _queue = new Queue(q.name, {
        prefix: this.namespace,
        connection: this.connection,
        ..._options,
      });

      this.queues.push(_queue);
    });

    return this.queues;
  }

  startWorkers(groupName: string | null = null) {
    let _queueMapsValues = Object.values(this._queueMap).filter((qp) => {
      if (groupName) {
        return qp.groupName === groupName;
      }
      return qp;
    });

    _queueMapsValues.forEach((qp) => {
      let exists = this.workers.find((w) => w.name === qp.name);
      if (!exists) {
        // Initialize worker instance

        let _options = {
          ...this._defaultWorkerOptions,
          ...(qp.workerOptions || {}),
        };
        let _w = new Worker(qp.name, qp.process, {
          connection: this.connection,
          prefix: this.namespace,
          ..._options,
        });

        // Attach event handlers
        if (qp?.onActive) {
          _w.on("active", qp.onActive);
        }

        if (qp?.onClosed) {
          _w.on("closed", qp.onClosed);
        }

        if (qp?.onCompleted) {
          _w.on("completed", qp.onCompleted);
        }

        if (qp?.onError) {
          _w.on("error", qp.onError);
        }
        if (qp?.onDrained) {
          _w.on("drained", qp.onDrained);
        }

        if (qp?.onFailed) {
          _w.on("failed", qp.onFailed);
        }

        if (qp?.onPaused) {
          _w.on("paused", qp.onPaused);
        }
        if (qp?.onProgress) {
          _w.on("progress", qp.onProgress);
        }
        if (qp?.onReady) {
          _w.on("ready", qp.onReady);
        }
        if (qp?.onStalled) {
          _w.on("stalled", qp.onStalled);
        }
        // Add to queues collection
        this.workers.push(_w);
      }
    });
  }

  // Add job

  async addJob<K extends keyof T>(
    name: K,
    jobname: string,
    payload: T[K] extends IQueueProcess<infer P> ? P : never,
    jobOptions?: JobsOptions
  ) {
    // Check item exists on `this._queueMap`
    let qMapItem = this.getFromMapByName(name);

    if (!qMapItem) {
      console.warn(`Queue ${String(name)} not found.`);
      return null;
    }
    // Get queue
    let queue = this.getQueueByName(qMapItem.name);

    if (!queue) {
      console.warn(`Queue ${String(name)} not initialized.`);
      return null;
    }

    let _jobOptions: JobsOptions = {
      ...this._defaultJobOptions,
      ...(qMapItem.defaultJobOptions || {}),
      ...(jobOptions || {}),
    };

    let job = await queue.add(jobname, payload, _jobOptions);

    return job;
  }

  // Add to scheduler

  async scheduleJob<K extends keyof T>(
    name: K,
    schedulerId: string,
    repeat: Omit<RepeatOptions, "key">,
    payload: {
      name: string;
      data?: T[K] extends IQueueProcess<infer P> ? P : never;
      opts?: JobsOptions;
    }
  ) {
    let qMapItem = this.getFromMapByName(name);
    if (!qMapItem) {
      console.warn(`Queue ${String(name)} not found.`);
      return null;
    }
    // Get queue
    let queue = this.getQueueByName(qMapItem.name);

    if (!queue) {
      console.warn(`Queue ${String(name)} not initialized.`);
      return null;
    }

    return await queue.upsertJobScheduler(schedulerId, repeat, payload);
  }

  async removeScheduleJob<K extends keyof T>(name: K, schedulerId: string) {
    let qMapItem = this.getFromMapByName(name);
    if (!qMapItem) {
      console.warn(`Queue ${String(name)} not found.`);
      return null;
    }
    // Get queue
    let queue = this.getQueueByName(qMapItem.name);

    if (!queue) {
      console.warn(`Queue ${String(name)} not initialized.`);
      return null;
    }

    return await queue.removeJobScheduler(schedulerId);
  }

  // Get all registered queues
  getQueues() {
    return this.queues;
  }

  // Get queue by name
  getQueue<K extends keyof T>(name: K) {
    let mapItem = this.getFromMapByName(name);
    if (!mapItem) return;

    return this.getQueueByName(mapItem.name);
  }

  // Get all registered workers
  getWorkers() {
    return this.workers;
  }

  // Get worker by name
  getWorker<K extends keyof T>(name: K) {
    let mapItem = this.getFromMapByName(name);
    if (!mapItem) return;

    return this.getWorkerByName(mapItem.name);
  }

  // Get IQueueProcess map item from `this.pmap`
  private getFromMapByName<K extends keyof T>(name: K) {
    let mapItem = this._queueMap[name];
    if (!mapItem) return;
    return mapItem as IQueueProcess;
  }

  // Get initialized Bullmq queue

  private getQueueByName(name: string) {
    let _queue = this.queues.find((queue) => queue.name === name);
    return _queue;
  }

  // Get initialized Bullmq worker
  private getWorkerByName(name: string) {
    let _worker = this.workers.find((worker) => worker.name === name);
    return _worker;
  }

  // Shutdown all
  async shutdown() {
    if (this.shutdownTriggered) {
      return;
    }

    this.shutdownTriggered = true;

    //Pause all workers
    if (this.workers?.length) {
      let $pauseWorkers = this.workers.map((w) => {
        return w.pause();
      });
      await Promise.all($pauseWorkers);
    }

    // Close all workers

    if (this.workers?.length) {
      let $closeWorkers = this.workers.map((w) => {
        return w.close();
      });
      await Promise.all($closeWorkers);
    }

    //Close all queues
    if (this.queues?.length) {
      let $closeQueues = this.queues.map((q) => {
        return q.close();
      });
      await Promise.all($closeQueues);
    }
  }

  setConnection(conn: ConnectionOptions) {
    this.connection = conn;
  }
}
