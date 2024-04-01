from app import webserver
from flask import request, jsonify
from app.task_runner import Task, Job_type
import os
import json
import re

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)
    else:
        # Method Not Allowed
        return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    print(f"JobID is {job_id}")
    # TODO
    # Check if job_id is valid
    job_id_copy = job_id
    id = job_id_copy.split("job_id")[-1]
    if int(id) > webserver.job_counter or int(id) < 1:
        return jsonify({"status": "error", "reason": "Invalid job_id"})
    
    # Check if job_id is done and return the result
    file_path = f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/{job_id}.json'
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            data = json.load(f)
            return jsonify({"status": "done", "data": data})

    # If not, return running status
    return jsonify({'status': 'running'})

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # Get request data
    data = request.json
    print(f"Got request {data}")

    # TODO
    request_data = request.json["question"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter, Job_type.states_mean, request_data, None)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    # TODO
    # Get request data
    request_question = request.json["question"]
    request_state = request.json["state"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter, Job_type.state_mean, request_question, request_state)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    # TODO
    # Get request data
    # request_data = request.json["question"].strip()
    request_data = request.json["question"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter, Job_type.best5, request_data, None)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    # TODO
    # Get request data
    request_data = request.json["question"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter, Job_type.worst5, request_data, None)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    # TODO
    # Get request data
    request_data = request.json["question"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter, Job_type.global_mean, request_data, None)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    # TODO
    # Get request data
    request_data = request.json["question"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter, Job_type.diff_from_mean, request_data, None)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    # TODO
    # Get request data
    request_data = request.json["question"]
    request_state = request.json["state"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter, Job_type.state_diff_from_mean, request_data, request_state)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    # TODO
    # Get request data
    request_data = request.json["question"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter, Job_type.mean_by_category, request_data, None)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    # TODO
    # Get request data
    request_data = request.json["question"]
    request_state = request.json["state"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter, Job_type.state_mean_by_category, request_data, request_state)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

# Implement graceful shutdown
@webserver.route('/api/shutdown', methods=['GET'])
def shutdown():
    webserver.tasks_runner.graceful_shutdown()

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
