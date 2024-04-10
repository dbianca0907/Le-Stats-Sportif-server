"""Module that manages the tasks and the thread pool."""
import os
from queue import Queue
from threading import Thread, Event
from enum import Enum
from app import job_executor

class JobType(Enum):
    """Enum that represents the type of job."""
    STATES_MEAN = 1
    STATE_MEAN = 2
    BEST5 = 3
    WORST5 = 4
    GLOBAL_MEAN = 5
    DIFF_FROM_MEAN = 6
    STATE_DIFF_FROM_MEAN = 7
    STATE_MEAN_BY_CATEGORY = 8
    MEAN_BY_CATEGORY = 9

class Status(Enum):
    """Enum that represents the status of the job."""
    RUNNING = 1
    DONE = 2

class Task:
    """Class that represents a task."""
    #pylint: disable=R0913
    def __init__(self, job_id, job_type: JobType, question, location, status: Status):
        self.job_id = job_id
        self.job_type = job_type
        self.question = question
        self.state = location
        self.status = status

class ThreadPool:
    """Class that manages the tasks and the thread pool."""
    def __init__(self, data_ingestor):
        self.num_of_threads = int(os.getenv('TP_NUM_OF_THREADS', os.cpu_count()))
        self.task_queue = Queue()
        self.workers = []
        self.terminate = Event()
        self.data_ingestor = data_ingestor
        self.jobs_status = {}

    def start(self):
        """Start the thread pool."""
        for _ in range(self.num_of_threads):
            task_runner = TaskRunner(self.task_queue, self.jobs_status,
                                     self.data_ingestor, self.terminate)
            self.workers.append(task_runner)
            task_runner.start()

    def submit_task(self, task):
        """Submit a task to the task queue."""
        self.task_queue.put(task)

    #pylint: disable=R0913
    def register_job(self, job_id, job_type, question, location, status):
        """Register a job in the task queue."""
        new_task = Task(job_id, job_type, question, location, status)
        self.jobs_status[job_id] = status
        self.submit_task(new_task)

    def graceful_shutdown(self):
        """Gracefully shutdown the thread pool."""
        self.terminate.set()
        self.task_queue.put(None)
        for worker in self.workers:
            worker.join()

class TaskRunner(Thread):
    """Class that solves the tasks."""
    def __init__(self, task_queue, job_status, data_ingestor, graceful_shutdown):
        super().__init__()
        self.task_queue = task_queue
        self.graceful_shutdown = graceful_shutdown
        self.data_ingestor = data_ingestor
        self.job_status = job_status

    def run(self):
        """ Run the task runner."""
        while 1:
            task = self.task_queue.get()
            if not self.graceful_shutdown.is_set() and task is not None:
                self.execute_task(task)
            if self.graceful_shutdown.is_set() and task is None:
                break

    def register_status(self, job_id):
        """Register the status of the job."""
        self.job_status[job_id] = Status.DONE


    def execute_task(self, task):
        """Execute the task from the queue."""
        df = self.data_ingestor.data_list
        job_exec = job_executor.JobExecutor()
        if task.job_type == JobType.STATES_MEAN:
            job_exec.states_mean(task, df)
        elif task.job_type == JobType.STATE_MEAN:
            job_exec.state_mean(task, df)
        elif task.job_type == JobType.BEST5:
            job_exec.best5(task, self.data_ingestor)
        elif task.job_type == JobType.WORST5:
            job_exec.worst5(task, df, self.data_ingestor)
        elif task.job_type == JobType.GLOBAL_MEAN:
            job_exec.global_mean(task, df)
        elif task.job_type == JobType.DIFF_FROM_MEAN:
            job_exec.diff_from_mean(task, df)
        elif task.job_type == JobType.STATE_MEAN_BY_CATEGORY:
            job_exec.state_mean_by_category(task, df)
        elif task.job_type == JobType.STATE_DIFF_FROM_MEAN:
            job_exec.state_diff_from_mean(task, df)
        elif task.job_type == JobType.MEAN_BY_CATEGORY:
            job_exec.mean_by_category(task, df)
        self.register_status(task.job_id)
