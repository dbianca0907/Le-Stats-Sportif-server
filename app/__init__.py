from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool

webserver = Flask(__name__)
webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

webserver.tasks_runner = ThreadPool(webserver.data_ingestor)

webserver.tasks_runner.start()

webserver.job_counter = 1

from app import routes
