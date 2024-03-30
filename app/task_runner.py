import time
import os
import json
from app.data_ingestor import DataIngestor
from queue import Queue
from threading import Thread, Event, Lock
from heapq import nlargest, nsmallest
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
    jobs = 9
    num_jobs = 10
    get_results = 11

class Task:
    def __init__(self, job_id, job_type: Job_type, question):
        self.job_id = job_id
        self.job_type = job_type
        self.question = question

class ThreadPool:
    def __init__(self):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task
        self.num_of_threads = int(os.getenv('TP_NUM_OF_THREADS', os.cpu_count()))
        self.task_queue = Queue()
        self.workers = []
        self.job_id_counter = 0
        self.lock = Lock()

    def start(self):
        for _ in range(self.num_of_threads):
            task_runner = TaskRunner(self.task_queue, self.lock)
            self.workers.append(task_runner)
            task_runner.start()
        
    def submit_task(self, task):
        self.task_queue.put(task)
    
    def register_job(self, job_id, job_type, question):
        new_task = Task(job_id, job_type, question)
        self.submit_task(new_task)

    def graceful_shutdown(self):
        for _ in range(self.num_of_threads):
            self.task_queue.put(None)
        for worker in self.workers:
            worker.join()

class TaskRunner(Thread):
    def __init__(self, task_queue, lock):
        # TODO: init necessary data structures
        super().__init__()
        self.task_queue = task_queue
        self.graceful_shutdown = Event()
        self.lock = lock

    def run(self):
        # while True:
            # TODO
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            while not self.graceful_shutdown.is_set():
                with self.lock:
                    task = self.task_queue.get()
                if task is not None:
                    self.execute_task(task)
    
    def execute_task(self, task):
        data_list = DataIngestor.data_list
        if task.job_type == Job_type.states_mean:
            # do the calculations
            pass
        elif task.job_type == Job_type.state_mean:
            pass
        elif task.job_type == Job_type.best5:
            sums = {}
            counts ={}
            for entry in data_list:
                if entry['Question'] == task.question:
                    # Verify if the year start and end is in range 2011 - 2022:
                    if 2011 <= int(entry['YearStart']) <= 2022 and 2011 <= int(entry['YearEnd']) <= 2022:
                        if entry['LocationDesc'] not in sums:
                            sums[entry['LocationDesc']] = 0
                            counts[entry['LocationDesc']] = 0
                        sums[entry['LocationDesc']] += float(entry['Data_Value'])
                        counts[entry['LocationDesc']] += 1
            averages = {location: sums[location] / counts[location] for location in sums}
            best5 = nlargest(5, averages, key=averages.get)
            best5_json = json.dumps({key: averages[key] for key in best5})
            with open(f'./results/{task.job_id}.json', 'w') as f:
                f.write(best5_json)
            pass
        elif task.job_type == Job_type.worst5:
            pass
        elif task.job_type == Job_type.global_mean:
            pass
        elif task.job_type == Job_type.diff_from_mean:
            pass
        elif task.job_type == Job_type.state_mean_by_category:
            pass
        elif task.job_type == Job_type.state_diff_from_mean:
            pass
        elif task.job_type == Job_type.jobs:
            pass
        elif task.job_type == Job_type.num_jobs:
            pass
        elif task.job_type == Job_type.get_results:
            pass


                     
