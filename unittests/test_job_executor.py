import unittest
import sys
import json
import os
import pandas as pd
# sys.path.append('app')
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../app')))
sys.path.append('./nutrition_activity_obesity_usa_subset.csv')
from unittest.mock import MagicMock, patch
from app.task_runner import Job_type, Task, Status
from app.job_executor import JobExecutor
from app import __init__
from flask import Flask

class TestJobExecutor(unittest.TestCase):
    def setUp(self):
        # Mock pentru DataIngestor
        self.mock_data_ingestor_instance = MagicMock()
        self.mock_data_ingestor_instance.questions_best_is_max = ['Test question']
        self.mock_data_ingestor_instance.questions_best_is_min = ['Another question']
        self.mock_data_ingestor_instance.data_list = pd.DataFrame([
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2018, 'LocationDesc': 'location 1', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 10.5},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2017, 'LocationDesc': 'location2', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 8.2},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2018, 'LocationDesc': 'location3', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 11.7},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2017, 'LocationDesc': 'location4', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 7.8},
            {'Question': 'Test question', 'YearStart': 2014, 'YearEnd': 2016, 'LocationDesc': 'location 1', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 9},
            {'Question': 'Test question', 'YearStart': 2011, 'YearEnd': 2019, 'LocationDesc': 'location2', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 11},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2018, 'LocationDesc': 'location 5', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 5},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2018, 'LocationDesc': 'location6', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 9.55},
            {'Question': 'Test question', 'YearStart': 2013, 'YearEnd': 2018, 'LocationDesc': 'location7', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 8.5}
        ])

    
    def test_best5(self):
        task_id = 'test_task_id'
        best5_expected = {"location3": 11.7, "location 1": 9.75, "location2": 9.6, "location6": 9.55, "location7": 8.5}

        # Configurarea TaskRunner-ului și a Task-ului mock
        mock_task = Task(job_id=task_id, job_type=Job_type.best5, question='Test question', location='location 1', status=Status.running)
        
        # Executarea Task-ului
        job_exec = JobExecutor()
        job_exec.best5(task=mock_task, data_ingestor=self.mock_data_ingestor_instance)
        file_path = f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json'
        with open(file_path, 'r') as file:
            best5_written = json.load(file)
        self.assertEqual(best5_expected, best5_written)
        os.remove(f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json')

if __name__ == '__main__':
    unittest.main()