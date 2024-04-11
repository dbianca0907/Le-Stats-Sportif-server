import unittest
import json
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
from unittest.mock import patch
from app.job_executor import JobExecutor


class TestJobExecutor(unittest.TestCase):
    @patch('app.data_ingestor.DataIngestor')
    def setUp(self, mock_data_ingestor_instance):
        # Mock pentru DataIngestor
        mock_data_ingestor_instance.questions_best_is_max = ['Test question']
        mock_data_ingestor_instance.questions_best_is_min = ['Another question']
        mock_data_ingestor_instance.data_list = pd.DataFrame([
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2018, 'LocationDesc': 'location 1', 'StratificationCategory1': 'test_category', 'Stratification1': 'test_stratification', 'Data_Value': 10.5},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2017, 'LocationDesc': 'location2', 'StratificationCategory1': 'test_category', 'Stratification1': 'test_stratification2', 'Data_Value': 8.2},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2018, 'LocationDesc': 'location3', 'StratificationCategory1': 'test_category', 'Stratification1': 'test', 'Data_Value': 11.7},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2017, 'LocationDesc': 'location4', 'StratificationCategory1': 'test_category', 'Stratification1': 'test', 'Data_Value': 7.8},
            {'Question': 'Test question', 'YearStart': 2014, 'YearEnd': 2016, 'LocationDesc': 'location 1', 'StratificationCategory1': 'test_category2', 'Stratification1': 'test_stratification2', 'Data_Value': 9},
            {'Question': 'Test question', 'YearStart': 2011, 'YearEnd': 2019, 'LocationDesc': 'location2', 'StratificationCategory1': 'test_category', 'Stratification1': 'test', 'Data_Value': 11},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2018, 'LocationDesc': 'location 5', 'StratificationCategory1': 'test_category', 'Stratification1': 'test_stratification', 'Data_Value': 5},
            {'Question': 'Test question', 'YearStart': 2012, 'YearEnd': 2018, 'LocationDesc': 'location6', 'StratificationCategory1': 'test_category', 'Stratification1': 'test', 'Data_Value': 9.55},
            {'Question': 'Test question', 'YearStart': 2013, 'YearEnd': 2018, 'LocationDesc': 'location7', 'StratificationCategory1': 'test_category', 'Stratification1': 'test_stratification2', 'Data_Value': 8.5}
        ])
        self.mock_data_ingestor_instance = mock_data_ingestor_instance

    @patch('app.task_runner.Task')
    @patch('app.task_runner.Status')
    def test_best5(self, mock_task, mock_status):
        task_id = 'test_task_id_best5'
        best5_expected = {"location3": 11.7, "location 1": 9.75, "location2": 9.6, "location6": 9.55, "location7": 8.5}
        mock_task.job_id = task_id
        mock_task.question = 'Test question'
        mock_task.status = mock_status.RUNNING

        job_exec = JobExecutor()
        job_exec.best5(task=mock_task, data_ingestor=self.mock_data_ingestor_instance)
        file_path = f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json'
        with open(file_path, 'r') as file:
            best5_written = json.load(file)

        self.assertEqual(best5_expected, best5_written)
        os.remove(f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json')
    
    @patch('app.task_runner.Task')
    @patch('app.task_runner.Status')
    def test_worst5(self, mock_task, mock_status):
        task_id = 'test_task_id_worst5'
        worst5_expected = {'location 5': 5.0, 'location4': 7.8, 'location7': 8.5, 'location6': 9.55, 'location2': 9.6}
        mock_task.job_id = task_id
        mock_task.question = 'Test question'
        mock_task.status = mock_status.RUNNING

        job_exec = JobExecutor()
        job_exec.worst5(task=mock_task, data_ingestor=self.mock_data_ingestor_instance)
        file_path = f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json'
        with open(file_path, 'r') as file:
            worst5_written = json.load(file)

        self.assertEqual(worst5_expected, worst5_written)
        os.remove(f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json')
    
    @patch('app.task_runner.Task')
    @patch('app.task_runner.Status')
    def test_states_mean(self, mock_task, mock_status):
        task_id = 'test_task_id_states_mean'
        states_mean_expected = {'location 5': 5.0, 'location4': 7.8, 'location7': 8.5, 'location6': 9.55, 
                                'location2': 9.6, 'location 1': 9.75, 'location3': 11.7}
        mock_task.job_id = task_id
        mock_task.question = 'Test question'
        mock_task.status = mock_status.RUNNING

        job_exec = JobExecutor()
        job_exec.states_mean(task=mock_task, df=self.mock_data_ingestor_instance.data_list)
        file_path = f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json'
        with open(file_path, 'r') as file:
            states_mean_written = json.load(file)
        self.assertEqual(states_mean_expected, states_mean_written)
        os.remove(f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json')
    
    @patch('app.task_runner.Task')
    @patch('app.task_runner.Status')
    def test_state_mean(self, mock_task, mock_status):
        task_id = 'test_task_id_state_mean'
        state_mean_expected = {'location 1': 9.75}
        mock_task.job_id = task_id
        mock_task.question = 'Test question'
        mock_task.state = 'location 1'
        mock_task.status = mock_status.RUNNING

        job_exec = JobExecutor()
        job_exec.state_mean(task=mock_task, df=self.mock_data_ingestor_instance.data_list)
        file_path = f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json'
        with open(file_path, 'r') as file:
            state_mean_written = json.load(file)

        self.assertEqual(state_mean_expected, state_mean_written)
        os.remove(f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json')
    
    @patch('app.task_runner.Task')
    @patch('app.task_runner.Status')
    def test_global_mean(self, mock_task, mock_status):
        task_id = 'test_task_id_global_mean'
        global_mean_expected = {'global_mean': 9.027777777777779}
        mock_task.job_id = task_id
        mock_task.question = 'Test question'
        mock_task.status = mock_status.RUNNING

        job_exec = JobExecutor()
        job_exec.global_mean(task=mock_task, df=self.mock_data_ingestor_instance.data_list)
        file_path = f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json'
        with open(file_path, 'r') as file:
            global_mean_written = json.load(file)

        self.assertEqual(global_mean_expected, global_mean_written)
        os.remove(f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json')
    
    @patch('app.task_runner.Task')
    @patch('app.task_runner.Status')
    def test_diff_from_mean(self, mock_task, mock_status):
        task_id = 'test_task_id_diff_from_mean'
        diff_from_mean_expected = {'location 5': 4.027777777777779, 'location4': 1.2277777777777787, 
                                   'location7': 0.5277777777777786, 'location6': -0.5222222222222221, 
                                   'location2': -0.5722222222222211, 'location 1': -0.7222222222222214, 
                                   'location3': -2.6722222222222207}
        mock_task.job_id = task_id
        mock_task.question = 'Test question'
        mock_task.status = mock_status.RUNNING

        job_exec = JobExecutor()
        job_exec.diff_from_mean(task=mock_task, df=self.mock_data_ingestor_instance.data_list)
        file_path = f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json'
        with open(file_path, 'r') as file:
            diff_from_mean_written = json.load(file)

        self.assertEqual(diff_from_mean_expected, diff_from_mean_written)
        os.remove(f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json')
    
    @patch('app.task_runner.Task')
    @patch('app.task_runner.Status')
    def test_state_diff_from_mean(self, mock_task, mock_status):
        task_id = 'test_task_id_state_diff_from_mean'
        state_diff_from_mean_expected = {'location 1': -0.7222222222222214}
        mock_task.job_id = task_id
        mock_task.question = 'Test question'
        mock_task.state = 'location 1'
        mock_task.status = mock_status.RUNNING

        job_exec = JobExecutor()
        job_exec.state_diff_from_mean(task=mock_task, df=self.mock_data_ingestor_instance.data_list)
        file_path = f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json'
        with open(file_path, 'r') as file:
            state_diff_from_mean_written = json.load(file)

        self.assertEqual(state_diff_from_mean_expected, state_diff_from_mean_written)
        os.remove(f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json')
    
    @patch('app.task_runner.Task')
    @patch('app.task_runner.Status')
    def test_state_mean_by_category(self, mock_task, mock_status):
        task_id = 'test_task_id_state_mean_by_category'
        state_mean_by_category_expected = {'location 1': {"('test_category', 'test_stratification')": 10.5, 
                                                            "('test_category2', 'test_stratification2')": 9.0}}
        mock_task.job_id = task_id
        mock_task.question = 'Test question'
        mock_task.state = 'location 1'
        mock_task.status = mock_status.RUNNING

        job_exec = JobExecutor()
        job_exec.state_mean_by_category(task=mock_task, df=self.mock_data_ingestor_instance.data_list)
        file_path = f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json'
        with open(file_path, 'r') as file:
            state_mean_by_category_written = json.load(file)

        self.assertEqual(state_mean_by_category_expected, state_mean_by_category_written)
        os.remove(f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json')
    
    @patch('app.task_runner.Task')
    @patch('app.task_runner.Status')
    def test_mean_by_categpry(self, mock_task, mock_status):
        task_id = 'test_task_id_mean_by_category'
        mean_by_category_expected = {"('location 1', 'test_category', 'test_stratification')": 10.5, 
                                    "('location 1', 'test_category2', 'test_stratification2')": 9.0,
                                    "('location 5', 'test_category', 'test_stratification')": 5.0, 
                                    "('location2', 'test_category', 'test')": 11.0, 
                                    "('location2', 'test_category', 'test_stratification2')": 8.2, 
                                    "('location3', 'test_category', 'test')": 11.7,
                                    "('location4', 'test_category', 'test')": 7.8,
                                    "('location6', 'test_category', 'test')": 9.55, 
                                    "('location7', 'test_category', 'test_stratification2')": 8.5}
        mock_task.job_id = task_id
        mock_task.question = 'Test question'
        mock_task.status = mock_status.RUNNING

        job_exec = JobExecutor()
        job_exec.mean_by_category(task=mock_task, df=self.mock_data_ingestor_instance.data_list)
        file_path = f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json'
        with open(file_path, 'r') as file:
            mean_by_category_written = json.load(file)

        self.assertEqual(mean_by_category_expected, mean_by_category_written)
        os.remove(f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/job_id{task_id}.json')


if __name__ == '__main__':
    unittest.main()