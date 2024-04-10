from app import webserver
from flask import request, jsonify
from app.task_runner import Status, JobType
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
    # Method Not Allowed
    return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    webserver.logger.info(f"Received request for job_id: {job_id}")
    # Check if job_id is valid
    job_id_copy = job_id
    id = job_id_copy.split("job_id")[-1]
    if int(id) > webserver.job_counter or int(id) < 1:
        return jsonify({"status": "error", "reason": "Invalid job_id"})

    #Check if job_id is done and return the result
    webserver.logger.info(f"Job status: {webserver.tasks_runner.jobs_status[int(id)]}")
    file_path = f'/Users/dumitru.bianca/Desktop/ASC/Le-Stats-Sportif-server/results/{job_id}.json'
    if os.path.exists(file_path) and webserver.tasks_runner.jobs_status[int(id)] == Status.DONE:
        with open(file_path, "r") as f:
            data = json.load(f)
            return jsonify({"status": "done", "data": data})

    # If not, return running status
    return jsonify({'status': 'running'})

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # Get request data
    webserver.logger.info("Got request from /api/states_mean")
    data = request.json
    print(f"Got request {data}")
    request_data = request.json["question"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter,
                                        JobType.STATES_MEAN, request_data,
                                        None, Status.RUNNING)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    webserver.logger.info("Got request from /api/state_mean")
    # Get request data
    request_question = request.json["question"]
    request_state = request.json["state"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter,
                                        JobType.STATE_MEAN, request_question,
                                        request_state, Status.RUNNING)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    webserver.logger.info("Got request from /api/best5")
    # Get request data
    # request_data = request.json["question"].strip()
    request_data = request.json["question"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter,
                                        JobType.BEST5, request_data,
                                        None, Status.RUNNING)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    webserver.logger.info("Got request from /api/worst5")
    # Get request data
    request_data = request.json["question"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter,
                                        JobType.WORST5, request_data,
                                        None, Status.RUNNING)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    webserver.logger.info("Got request from /api/global_mean")
    # Get request data
    request_data = request.json["question"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter,
                                        JobType.GLOBAL_MEAN, request_data,
                                        None, Status.RUNNING)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    webserver.logger.info("Got request from /api/diff_from_mean")
    # Get request data
    request_data = request.json["question"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter,
                                        JobType.DIFF_FROM_MEAN, request_data,
                                        None, Status.RUNNING)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    webserver.logger.info("Got request from /api/state_diff_from_mean")
    # Get request data
    request_data = request.json["question"]
    request_state = request.json["state"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter,
                                        JobType.STATE_DIFF_FROM_MEAN, request_data,
                                        request_state, Status.RUNNING)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    webserver.logger.info("Got request from /api/mean_by_category")
    # Get request data
    request_data = request.json["question"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter,
                                        JobType.MEAN_BY_CATEGORY, request_data,
                                        None, Status.RUNNING)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    webserver.logger.info("Got request from /api/state_mean_by_category")
    # Get request data
    request_data = request.json["question"]
    request_state = request.json["state"]
    # Register job. Don't wait for task to finish
    webserver.tasks_runner.register_job(webserver.job_counter,
                                        JobType.STATE_MEAN_BY_CATEGORY, request_data,
                                        request_state, Status.RUNNING)
    # Increment job_id counter
    webserver.job_counter += 1
    # Return associated job_id
    return jsonify({"job_id": "job_id" + str(webserver.job_counter - 1)})

# Implement graceful shutdown
@webserver.route('/api/shutdown', methods=['GET'])
def shutdown():
    webserver.logger.info("Got request from /api/shutdown")
    webserver.tasks_runner.graceful_shutdown()

@webserver.route('/api/jobs', methods=['GET'])
def get_status():
    webserver.logger.info("Got request from /api/jobs")
    jobs_status = webserver.tasks_runner.jobs_status
    data = [{"job_id_" + str(job_id): status} for i, (job_id, status) in enumerate(jobs_status.items())]
    return jsonify({"status": "done", "data": data})

@webserver.route('/api/num_jobs', methods=['GET'])
def get_num_jobs():
    webserver.logger.info("Got request from /api/num_jobs")
    num_jobs = 0
    if webserver.tasks_runner.terminate is not set:
        num_jobs = len(webserver.tasks_runner.task_queue)
    return jsonify({"num_jobs": num_jobs})


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
