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
        print (f"Starting thread pool with {self.num_of_threads} threads")
        for _ in range(self.num_of_threads):
            task_runner = TaskRunner(self.task_queue, self.data_ingestor, self.lock, self.terminate)
            self.workers.append(task_runner)
            task_runner.start()
            print(f"Thread {task_runner.ident} started")
        print(f"Thread pool started with {len(self.workers)} threads")
        
    def submit_task(self, task):
        self.task_queue.put(task)
        print(f"Task {task.job_id} submitted")
    
    def register_job(self, job_id, job_type, question, state):
        new_task = Task(job_id, job_type, question, state)
        self.submit_task(new_task)

    def graceful_shutdown(self):
        self.terminate.set()
        print(f"Graceful shutdown initiated will close {len(self.workers)} threads")
        for worker in self.workers:
            worker.join()
            print(f"Thread {worker.ident} joined")

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
            with self.lock:
                task = self.task_queue.get()
            if task is not None:
                self.execute_task(task)
                print(f"Task {task.job_id} executed")
            print(f"Thread {self.ident} is running")
        print(f"Thread {self.ident} is shutting down")
    
    def execute_task(self, task):
        data_list = self.data_ingestor.data_list
        if task.job_type == Job_type.states_mean:
            question = task.question
            sums = {}
            counts ={}
            for entry in data_list:
                if entry['Question'] == question:
                    # Verify if the year start and end is in range 2011 - 2022:
                    if 2011 <= entry['YearStart'] <= 2022 and 2011 <= entry['YearEnd'] <= 2022:
                        if entry['LocationDesc'] not in sums:
                            sums[entry['LocationDesc']] = 0
                            counts[entry['LocationDesc']] = 0
                        sums[entry['LocationDesc']] += entry['Data_Value']
                        counts[entry['LocationDesc']] += 1
            averages = {location: sums[location] / counts[location] for location in sums}
            averages_sorted = dict(sorted(averages.items(), key=lambda item: item[1]))
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
            mean = 0
            sum = 0
            count = 0
            for entry in data_list:
                if entry['Question'] == question and entry['LocationDesc'] == state:
                    # Verify if the year start and end is in range 2011 - 2022:
                    if 2011 <= entry['YearStart'] <= 2022 and 2011 <= entry['YearEnd'] <= 2022:
                        sum += entry['Data_Value']
                        count += 1
            mean = sum / count
            state_mean_json = json.dumps({state: mean})
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(state_mean_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.best5:
            question = task.question
            sums = {}
            counts ={}
            for entry in data_list:
                if entry['Question'] == question:
                    # Verify if the year start and end is in range 2011 - 2022:
                    if 2011 <= entry['YearStart'] <= 2022 and 2011 <= entry['YearEnd'] <= 2022:
                        if entry['LocationDesc'] not in sums:
                            sums[entry['LocationDesc']] = 0
                            counts[entry['LocationDesc']] = 0
                        sums[entry['LocationDesc']] += entry['Data_Value']
                        counts[entry['LocationDesc']] += 1
            averages = {location: sums[location] / counts[location] for location in sums}
            if question in self.data_ingestor.questions_best_is_max:
                averages_sorted = dict(sorted(averages.items(), key=lambda item: item[1], reverse=True))
            else:
                averages_sorted = dict(sorted(averages.items(), key=lambda item: item[1]))
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
            sums = {}
            counts ={}
            for entry in data_list:
                if entry['Question'] == question:
                    # Verify if the year start and end is in range 2011 - 2022:
                    if 2011 <= entry['YearStart'] <= 2022 and 2011 <= entry['YearEnd'] <= 2022:
                        if entry['LocationDesc'] not in sums:
                            sums[entry['LocationDesc']] = 0
                            counts[entry['LocationDesc']] = 0
                        sums[entry['LocationDesc']] += entry['Data_Value']
                        counts[entry['LocationDesc']] += 1
            averages = {location: sums[location] / counts[location] for location in sums}
            if question in self.data_ingestor.questions_best_is_min:
                averages_sorted = dict(sorted(averages.items(), key=lambda item: item[1], reverse=True))
            else:
                averages_sorted = dict(sorted(averages.items(), key=lambda item: item[1]))
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
            sums = 0
            counts = 0
            mean = 0
            for entry in data_list:
                if entry['Question'] == question:
                    # Verify if the year start and end is in range 2011 - 2022:
                    if 2011 <= entry['YearStart'] <= 2022 and 2011 <= entry['YearEnd'] <= 2022:
                        sums += entry['Data_Value']
                        counts += 1
            mean = sums / counts
            global_mean_json = json.dumps({"global_mean": mean})
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(global_mean_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.diff_from_mean:
            question = task.question
            sums = {}
            counts ={}
            sums_global = 0
            counts_global = 0
            mean_global = 0
            for entry in data_list:
                if entry['Question'] == question:
                    # Verify if the year start and end is in range 2011 - 2022:
                    if 2011 <= entry['YearStart'] <= 2022 and 2011 <= entry['YearEnd'] <= 2022:
                        sums_global += entry['Data_Value']
                        counts_global += 1
                        if entry['LocationDesc'] not in sums:
                            sums[entry['LocationDesc']] = 0
                            counts[entry['LocationDesc']] = 0
                        sums[entry['LocationDesc']] += entry['Data_Value']
                        counts[entry['LocationDesc']] += 1
            mean_global = sums_global / counts_global
            averages = {location: sums[location] / counts[location] for location in sums}
            diff_from_mean = {location: mean_global - averages[location] for location in averages}
            diff_from_mean_sorted = dict(sorted(diff_from_mean.items(), key=lambda item: item[1], reverse=True))
            diff_from_mean_json = json.dumps(diff_from_mean_sorted)
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(diff_from_mean_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.state_mean_by_category:
            averages_dict = {}
            for entry in data_list:
                if entry['LocationDesc'] == task.state and entry['Question'] == task.question:
                    key = (entry['StratificationCategory1'], entry['Stratification1'])
                    if key not in averages_dict:
                        averages_dict[key] = {'sum': 0, 'count': 0, 'mean': 0}
                    else:
                        averages_dict[key]['sum'] += entry['Data_Value']
                        averages_dict[key]['count'] += 1
            averages = {
                str(key): averages_dict[key]['sum'] / averages_dict[key]['count'] if averages_dict[key]['mean'] != 0 else 1
                for key in averages_dict
            }

            state_averages_json = json.dumps({task.state: averages})
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    print("INAINTE DE SCRIERE:", state_averages_json)
                    f.write(state_averages_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.state_diff_from_mean:
            question = task.question
            state = task.state
            mean = 0
            sum = 0
            count = 0
            sum_global = 0
            count_global = 0
            mean_global = 0
            for entry in data_list:
                if entry['Question'] == question and 2011 <= entry['YearStart'] <= 2022 and 2011 <= entry['YearEnd'] <= 2022:
                    sum_global += entry['Data_Value']
                    count_global += 1
                    # Verify if the year start and end is in range 2011 - 2022:
                    if  entry['LocationDesc'] == state:
                        sum += entry['Data_Value']
                        count += 1
            mean = sum / count
            mean_global = sum_global / count_global
            state_mean_json = json.dumps({state: mean_global - mean})
            file_path = f'./results/job_id{task.job_id}.json'
            try:
                with open(file_path, 'w') as f:
                    f.write(state_mean_json)
            except Exception as e:
                print(f"An error occurred while writing to file: {e}")
        elif task.job_type == Job_type.mean_by_category:
            averages_dict = {}
            for entry in data_list:
                key = None
                if entry['Question'] == task.question:
                    key = (entry['LocationDesc'], entry['StratificationCategory1'], entry['Stratification1'])
                if key not in averages_dict:
                    averages_dict[key] = {'sum': 0, 'count': 0, 'mean': 0}
                else:
                    averages_dict[key]['sum'] += entry['Data_Value']
                    averages_dict[key]['count'] += 1
            averages = {
                str(key): averages_dict[key]['sum'] / averages_dict[key]['count'] if averages_dict[key]['mean'] != 0 else 1
                for key in averages_dict
            }

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


                     
