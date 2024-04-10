import os
import app.job_executor as job_executor
from queue import Queue
from threading import Thread, Event
from enum import Enum


class Job_type(Enum):
    states_mean = 1
    state_mean = 2
    best5 = 3
    worst5 = 4
    global_mean = 5
    diff_from_mean = 6
    state_diff_from_mean = 7
    state_mean_by_category = 8
    mean_by_category = 9

class Status(Enum):
    running = 1
    done = 2

class Task:
    def __init__(self, job_id, job_type: Job_type, question, location, status: Status):
        self.job_id = job_id
        self.job_type = job_type
        self.question = question
        self.state = location
        self.status = status

class ThreadPool:
    def __init__(self, data_ingestor):
        self.num_of_threads = int(os.getenv('TP_NUM_OF_THREADS', os.cpu_count()))
        self.task_queue = Queue()
        self.workers = []
        self.terminate = Event()
        self.data_ingestor = data_ingestor
        self.jobs_status = {}

    def start(self):
        for _ in range(self.num_of_threads):
            task_runner = TaskRunner(self.task_queue, self.jobs_status, self.data_ingestor, self.terminate)
            self.workers.append(task_runner)
            task_runner.start()
        
    def submit_task(self, task):
        self.task_queue.put(task)
    
    def register_job(self, job_id, job_type, question, location, status):
        new_task = Task(job_id, job_type, question, location, status)
        self.jobs_status[job_id] = status
        self.submit_task(new_task)

    def graceful_shutdown(self):
        self.terminate.set()
        self.task_queue.append(None)
        for worker in self.workers:
            worker.join()

class TaskRunner(Thread):
    def __init__(self, task_queue, job_status, data_ingestor, graceful_shutdown):
        super().__init__()
        self.task_queue = task_queue
        self.graceful_shutdown = graceful_shutdown
        self.data_ingestor = data_ingestor
        self.job_status = job_status

    def run(self):
        while (1):
            task = self.task_queue.get()
            if not self.graceful_shutdown.is_set() and task is not None:
                self.execute_task(task)
            if self.graceful_shutdown.is_set() and task is None:
                break
    
    def register_status(self, job_id):
        self.job_status[job_id] = Status.done

    
    def execute_task(self, task):
        df = self.data_ingestor.data_list
        job_exec = job_executor.JobExecutor()
        if task.job_type == Job_type.states_mean:
            job_exec.states_mean(task, df)
        elif task.job_type == Job_type.state_mean:
            job_exec.state_mean(task, df)
        elif task.job_type == Job_type.best5:
            job_exec.best5(task, self.data_ingestor)
        elif task.job_type == Job_type.worst5:
            job_exec.worst5(task, df, self.data_ingestor)
        elif task.job_type == Job_type.global_mean:
            job_exec.global_mean(task, df)
        elif task.job_type == Job_type.diff_from_mean:
            job_exec.diff_from_mean(task, df)
        elif task.job_type == Job_type.state_mean_by_category:
            job_exec.state_mean_by_category(task, df)
        elif task.job_type == Job_type.state_diff_from_mean:
            job_exec.state_diff_from_mean(task, df)
        elif task.job_type == Job_type.mean_by_category:
            job_exec.mean_by_category(task, df)
        self.register_status(task.job_id)