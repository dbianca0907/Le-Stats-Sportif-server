from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool
from logging.handlers import RotatingFileHandler
import logging
import os

webserver = Flask(__name__)
webserver.data_ingestor = DataIngestor()

webserver.tasks_runner = ThreadPool(webserver.data_ingestor)

webserver.logger = logging.Logger(__name__)
handler = RotatingFileHandler("logg/file.log", maxBytes=10000, backupCount=10)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', \
                              datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)

webserver.logger.addHandler(handler)

webserver.logger.setLevel(logging.INFO)

webserver.tasks_runner.start()

webserver.job_counter = 1

director = os.path.dirname('results/')
if not os.path.exists(director):
    os.makedirs(director)

from app import routes
