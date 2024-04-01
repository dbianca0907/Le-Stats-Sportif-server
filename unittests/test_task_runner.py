import unittest
import sys
import json
import os
sys.path.append('app')
from unittest.mock import patch, mock_open, MagicMock
from app.task_runner import Job_type, TaskRunner, Task
from app.__init__ import webserver

class TestTaskRunner(unittest.TestCase):

    @patch('app.task_runner.DataIngestor')
    def test_execute_task_best5(self, mock_data_ingestor):
        # Definiți datele necesare pentru testare
        task_id = 'test_task_id'
        best5_expected = {"location3": 11.7, "location 1": 9.75, "location2": 9.6, "location6": 9.55, "location7": 8.5}

        mock_data_ingestor.data_list = [
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2018, 'LocationDesc': 'location 1', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 10.5},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2017, 'LocationDesc': 'location2', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 8.2},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2018, 'LocationDesc': 'location3', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 11.7},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2017, 'LocationDesc': 'location4', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 7.8},
            {'Question': 'Test question', 'YearStart': 2014, 'YearEnd': 2016, 'LocationDesc': 'location 1', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 9},
            {'Question': 'Test question', 'YearStart': 2011, 'YearEnd': 2019, 'LocationDesc': 'location2', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 11},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2018, 'LocationDesc': 'location 5', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 5},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2018, 'LocationDesc': 'location6', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 9.55},
            {'Question': 'Test question', 'YearStart': 2013, 'YearEnd': 2018, 'LocationDesc': 'location7', 'StratificationCategory1': 'test', 'Stratification1': 'test', 'Data_Value': 8.5},
        ]

        task_runner = TaskRunner(None, mock_data_ingestor, None, None)
        mock_task = Task(job_id=task_id, job_type=Job_type.best5, question='Test question')
        task_runner.execute_task(mock_task)

        with open(f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json', 'r') as file:
            best5_written = json.load(file)

        self.assertEqual(best5_expected, best5_written)
        os.remove(f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json')

if __name__ == '__main__':
    unittest.main()