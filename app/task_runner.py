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
        director = os.path.dirname('results/')
        if not os.path.exists(director):
            os.makedirs(director)
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
            if not self.graceful_shutdown.is_set() or task is not None:
                self.execute_task(task)
            if self.graceful_shutdown.is_set() and task is None:
                break
    
    def register_status(self, job_id):
        self.job_status[job_id] = Status.done
    
    def write_result(self, json_file, job_id):
        file_path = f'./results/job_id{job_id}.json'
        try:
            with open(file_path, 'w') as f:
                f.write(json_file)
                self.register_status(job_id)
        except Exception as e:
            print(f"An error occurred while writing to file: {e}")

    
    def execute_task(self, task):
        df = self.data_ingestor.data_list
        if task.job_type == Job_type.states_mean:
            self.states_mean(task, df)
        elif task.job_type == Job_type.state_mean:
            self.state_mean(task, df)
        elif task.job_type == Job_type.best5:
            self.best5(task, df)
        elif task.job_type == Job_type.worst5:
            self.worst5(task, df)
        elif task.job_type == Job_type.global_mean:
            self.global_mean(task, df)
        elif task.job_type == Job_type.diff_from_mean:
            self.diff_from_mean(task, df)
        elif task.job_type == Job_type.state_mean_by_category:
            self.state_mean_by_category(task, df)
        elif task.job_type == Job_type.state_diff_from_mean:
            self.state_diff_from_mean(task, df)
        elif task.job_type == Job_type.mean_by_category:
            self.mean_by_category(task, df)


    def states_mean(self, task, df):
        question = task.question
        df_filtered = df[(df['Question'] == question) & 
            (df['YearStart'] >= 2011) & 
            (df['YearEnd'] <= 2022)]
        averages = df_filtered.groupby('LocationDesc')['Data_Value'].mean()
        averages_sorted = averages.sort_values().to_dict()
        states_mean_json = json.dumps(averages_sorted)
        self.write_result(states_mean_json, task.job_id)
    
    def state_mean(self, task, df):
        question = task.question
        state = task.state
        df_filtered = df[(df['Question'] == question) & (df['LocationDesc'] == state)]
        mean = df_filtered['Data_Value'].mean()
        state_mean_json = json.dumps({state: mean})
        self.write_result(state_mean_json, task.job_id)

    def best5(self, task, df):
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
        self.write_result(best5_json, task.job_id)
    
    def worst5(self, task, df):
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
        self.write_result(best5_json, task.job_id)
    
    def global_mean(self, task, df):
        question = task.question
        df_filtered = df[(df['Question'] == question) &
            (df['YearStart'] >= 2011) &
            (df['YearEnd'] <= 2022)]
        global_mean = df_filtered['Data_Value'].mean()
        global_mean_json = json.dumps({"global_mean": global_mean})
        self.write_result(global_mean_json, task.job_id)
    
    def diff_from_mean(self, task, df):
        question = task.question
        df_filtered = df[(df['Question'] == question) &
            (df['YearStart'] >= 2011) &
            (df['YearEnd'] <= 2022)]
        global_mean = df_filtered['Data_Value'].mean()
        averages = df_filtered.groupby('LocationDesc')['Data_Value'].mean().sort_values().to_dict()
        diff_from_mean = {location: global_mean - averages[location] for location in averages}
        diff_from_mean_json = json.dumps(diff_from_mean)
        self.write_result(diff_from_mean_json, task.job_id)
    
    def state_mean_by_category(self, task, df):
        df_filtered = df[(df['Question'] == task.question) & (df['LocationDesc'] == task.state)].copy()
        grouped = df_filtered.groupby(['StratificationCategory1', 'Stratification1'])['Data_Value'].mean()
        grouped_dict = grouped.to_dict()
        averages = {str(key): grouped_dict[key] for key in grouped_dict}
        averages_json = json.dumps({task.state : averages})
        self.write_result(averages_json, task.job_id)
    
    def state_diff_from_mean(self, task, df):
        question = task.question 
        df_filtered = df[(df['Question'] == question) &
            (df['YearStart'] >= 2011) &
            (df['YearEnd'] <= 2022)]
        global_mean = df_filtered['Data_Value'].mean()
        df_new = df_filtered[(df['LocationDesc'] == task.state)].copy()
        averages = df_new.groupby('LocationDesc')['Data_Value'].mean()
        averages_dict = averages.to_dict()
        diff_from_mean_json = json.dumps({task.state: global_mean - averages_dict[task.state]})
        self.write_result(diff_from_mean_json, task.job_id)

    def mean_by_category(self, task, df):
        df_filtered = df[df['Question'] == task.question]
        grouped = df_filtered.groupby(['LocationDesc', 'StratificationCategory1', 'Stratification1'])['Data_Value'].mean()
        grouped_dict = grouped.to_dict()
        averages = {str(key): grouped_dict[key] for key in grouped_dict}
        averages_json = json.dumps(averages)
        self.write_result(averages_json, task.job_id)
                     