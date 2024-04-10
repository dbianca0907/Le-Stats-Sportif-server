import logging
import os
from flask import Flask
from logging.handlers import RotatingFileHandler
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool

webserver = Flask(__name__)
webserver.data_ingestor = DataIngestor()

webserver.tasks_runner = ThreadPool(webserver.data_ingestor)

#if there are any files in logg directory delete them
logg_dir = os.path.join(os.path.dirname(__file__), '../logg')
for file in os.listdir(logg_dir):
    if file.startswith('file'):
        os.remove(os.path.join(logg_dir, file))
webserver.logger = logging.Logger(__name__)
handler = RotatingFileHandler("logg/file.log", maxBytes=100000, backupCount=10)
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
