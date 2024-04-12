import unittest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from unittest.mock import patch
from app import webserver
from app.task_runner import ThreadPool

class TestGracefulShutdown(unittest.TestCase):
    @patch('app.webserver.tasks_runner.terminate.is_set', return_value=False)
    def test_graceful_shutdown_a(self, mock_tasks_runner):
        response = webserver.test_client().get('/api/shutdown')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json, {"status": "shutting_down"})
    
    @patch('app.webserver.tasks_runner.terminate.is_set', return_value=True)
    def test_graceful_shutdown_failure(self, mock_tasks_runner):
        response = webserver.test_client().get('/api/shutdown')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json,{"status": "error", "message": "Server already shutting down"})
    
    @patch('app.webserver.tasks_runner.terminate.is_set', return_value=True)
    def test_get_result_invalid_request(self, mock_tasks_runner):
        response = webserver.test_client().get('/api/get_results/job_id1')
        self.assertEqual(response.json, {"status": "error", "message": "Invalid request"})
    
    @patch('app.webserver.tasks_runner.terminate.is_set', return_value=True)
    def test_states_mean_invalid_request(self, mock_tasks_runner):
        response = webserver.test_client().post('/api/states_mean')
        self.assertEqual(response.json, {"status": "error", "message": "Invalid request"})
    
    @patch('app.webserver.tasks_runner.terminate.is_set', return_value=True)
    def test_state_mean_invalid_request(self, mock_tasks_runner):
        response = webserver.test_client().post('/api/state_mean')
        self.assertEqual(response.json, {"status": "error", "message": "Invalid request"})
    
    @patch('app.webserver.tasks_runner.terminate.is_set', return_value=True)
    def test_best5_invalid_request(self, mock_tasks_runner):
        response = webserver.test_client().post('/api/best5')
        self.assertEqual(response.json, {"status": "error", "message": "Invalid request"})
    
    @patch('app.webserver.tasks_runner.terminate.is_set', return_value=True)
    def test_worst5_invalid_request(self, mock_tasks_runner):
        response = webserver.test_client().post('/api/worst5')
        self.assertEqual(response.json, {"status": "error", "message": "Invalid request"})
    
    @patch('app.webserver.tasks_runner.terminate.is_set', return_value=True)
    def test_global_mean_invalid_request(self, mock_tasks_runner):
        response = webserver.test_client().post('/api/global_mean')
        self.assertEqual(response.json, {"status": "error", "message": "Invalid request"})
    
    @patch('app.webserver.tasks_runner.terminate.is_set', return_value=True)
    def test_diff_from_mean_invalid_request(self, mock_tasks_runner):
        response = webserver.test_client().post('/api/diff_from_mean')
        self.assertEqual(response.json, {"status": "error", "message": "Invalid request"})
    
    @patch('app.webserver.tasks_runner.terminate.is_set', return_value=True)
    def test_state_diff_from_mean_invalid_request(self, mock_tasks_runner):
        response = webserver.test_client().post('/api/state_diff_from_mean')
        self.assertEqual(response.json, {"status": "error", "message": "Invalid request"})
    
    @patch('app.webserver.tasks_runner.terminate.is_set', return_value=True)
    def test_mean_by_category(self, mock_tasks_runner):
        response = webserver.test_client().post('/api/mean_by_category')
        self.assertEqual(response.json, {"status": "error", "message": "Invalid request"})
    
    @patch('app.webserver.tasks_runner.terminate.is_set', return_value=True)
    def test_state_mean_by_category(self, mock_tasks_runner):
        response = webserver.test_client().post('/api/state_mean_by_category')
        self.assertEqual(response.json, {"status": "error", "message": "Invalid request"})

