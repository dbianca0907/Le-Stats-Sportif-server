import time
import os
import json
import pandas as pd
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
    mean_by_category = 9
    jobs = 10
    num_jobs = 11
    get_results = 12

class Task:
    def __init__(self, job_id, job_type: Job_type, question, location):
        self.job_id = job_id
        self.job_type = job_type
        self.question = question
        self.state = location

class ThreadPool:
    def __init__(self, data_ingestor):
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
        self.lock = Lock()
        self.terminate = Event()
        self.data_ingestor = data_ingestor

    def start(self):
        director = os.path.dirname('results/')
        if not os.path.exists(director):
            os.makedirs(director)
        for _ in range(self.num_of_threads):
            task_runner = TaskRunner(self.task_queue, self.data_ingestor, self.lock, self.terminate)
            self.workers.append(task_runner)
            task_runner.start()
        
    def submit_task(self, task):
        self.task_queue.put(task)
    
    def register_job(self, job_id, job_type, question, state):
        new_task = Task(job_id, job_type, question, state)
        self.submit_task(new_task)

    def graceful_shutdown(self):
        self.terminate.set()
        for worker in self.workers:
            worker.join()

class TaskRunner(Thread):
    def __init__(self, task_queue, data_ingestor, lock, graceful_shutdown):
        # TODO: init necessary data structures
        super().__init__()
        self.task_queue = task_queue
        self.graceful_shutdown = graceful_shutdown
        self.lock = lock
        self.data_ingestor = data_ingestor

    def run(self):
        while not self.graceful_shutdown.is_set():
            # TODO
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            
            task = None
            # self.lock.acquire()
            task = self.task_queue.get()
            # self.lock.release()
            if task is not None:
                self.execute_task(task)
    
    def execute_task(self, task):
        df = self.data_ingestor.data_list
        if task.job_type == Job_type.states_mean:
            question = task.question
            df_filtered = df[(df['Question'] == question) & 
                (df['YearStart'] >= 2011) & 
                (df['YearEnd'] <= 2022)]
            averages = df_filtered.groupby('LocationDesc')['Data_Value'].mean()
            averages_sorted = averages.sort_values().to_dict()
            states_mean_json = json.dumps(averages_sorted)
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(states_mean_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.state_mean:
            question = task.question
            state = task.state
            df_filtered = df[(df['Question'] == question) & (df['LocationDesc'] == state)]
            mean = df_filtered['Data_Value'].mean()
            state_mean_json = json.dumps({state: mean})
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(state_mean_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.best5:
            question = task.question
            df_filtered = df[(df['Question'] == question) & 
                (df['YearStart'] >= 2011) & 
                (df['YearEnd'] <= 2022)]
            averages = df_filtered.groupby('LocationDesc')['Data_Value'].mean()
            if question in self.data_ingestor.questions_best_is_max:
                averages_sorted = averages.sort_values(ascending=False).to_dict()
            else:
                averages_sorted = averages.sort_values().to_dict()
            best5 = dict(list(averages_sorted.items())[:5])
            best5_json = json.dumps(best5)
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(best5_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.worst5:
            question = task.question
            df_filtered = df[(df['Question'] == question) & 
                (df['YearStart'] >= 2011) & 
                (df['YearEnd'] <= 2022)]
            averages = df_filtered.groupby('LocationDesc')['Data_Value'].mean()
            if question in self.data_ingestor.questions_best_is_min:
                averages_sorted = averages.sort_values(ascending=False).to_dict()
            else:
                averages_sorted = averages.sort_values().to_dict()
            best5 = dict(list(averages_sorted.items())[:5])
            best5_json = json.dumps(best5)
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(best5_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.global_mean:
            question = task.question
            df_filtered = df[(df['Question'] == question) &
                (df['YearStart'] >= 2011) &
                (df['YearEnd'] <= 2022)]
            global_mean = df_filtered['Data_Value'].mean()
            global_mean_json = json.dumps({"global_mean": global_mean})
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(global_mean_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.diff_from_mean:
            question = task.question
            df_filtered = df[(df['Question'] == question) &
                (df['YearStart'] >= 2011) &
                (df['YearEnd'] <= 2022)]
            global_mean = df_filtered['Data_Value'].mean()
            averages = df_filtered.groupby('LocationDesc')['Data_Value'].mean().sort_values().to_dict()
            diff_from_mean = {location: global_mean - averages[location] for location in averages}
            diff_from_mean_json = json.dumps(diff_from_mean)
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(diff_from_mean_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.state_mean_by_category:
            df_filtered = df[(df['Question'] == task.question) & (df['LocationDesc'] == task.state)]
            grouped = df_filtered.groupby(['StratificationCategory1', 'Stratification1'])['Data_Value'].mean()
            grouped_dict = grouped.to_dict()
            averages = {str(key): grouped_dict[key] for key in grouped_dict}
            averages_json = json.dumps({task.state : averages})
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(averages_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.state_diff_from_mean:
            question = task.question 
            df_filtered = df[(df['Question'] == question) &
                (df['YearStart'] >= 2011) &
                (df['YearEnd'] <= 2022)]
            global_mean = df_filtered['Data_Value'].mean()
            averages = df_filtered[(df['LocationDesc'] == task.state)].groupby('LocationDesc')['Data_Value'].mean()
            averages_dict = averages.to_dict()
            diff_from_mean_json = json.dumps({task.state: global_mean - averages_dict[task.state]})
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(diff_from_mean_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.mean_by_category:
            df_filtered = df[df['Question'] == task.question]
            grouped = df_filtered.groupby(['LocationDesc', 'StratificationCategory1', 'Stratification1'])['Data_Value'].mean()
            grouped_dict = grouped.to_dict()
            averages = {str(key): grouped_dict[key] for key in grouped_dict}
            averages_json = json.dumps(averages)
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(averages_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.jobs:
            pass
        elif task.job_type == Job_type.num_jobs:
            pass
        elif task.job_type == Job_type.get_results:
            pass


                     
