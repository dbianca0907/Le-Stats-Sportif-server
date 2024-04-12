"""Module that contains the utilities for executing the tasks."""

import json

class JobExecutor:
    """Class that executes the tasks and writes the results to a file."""
    def __init__(self, logger):
        self.logger = logger

    def write_result(self, json_file, job_id):
        """Write the result to a file."""
        file_path = f'./results/job_id{job_id}.json'
        self.logger.info(f"Writing to file: {file_path}")
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(json_file)
        except FileNotFoundError as e:
            self.logger.info(f"An error occurred while writing to file: {e}")

    def states_mean(self, task, df):
        """Calculate the mean of the data values for each state."""
        question = task.question
        df_filtered = df[(df['Question'] == question) &
            (df['YearStart'] >= 2011) &
            (df['YearEnd'] <= 2022)]
        averages = df_filtered.groupby('LocationDesc')['Data_Value'].mean()
        averages_sorted = averages.sort_values().to_dict()
        states_mean_json = json.dumps(averages_sorted)
        self.logger.info(f"States mean were calculated for {task.job_id}")
        self.write_result(states_mean_json, task.job_id)

    def state_mean(self, task, df):
        """Calculate the mean of the data values for a specific state."""
        question = task.question
        state = task.state
        df_filtered = df[(df['Question'] == question) & (df['LocationDesc'] == state)]
        mean = df_filtered['Data_Value'].mean()
        state_mean_json = json.dumps({state: mean})
        self.logger.info(f"State mean for {state} in {task.job_id}")
        self.write_result(state_mean_json, task.job_id)

    def best5(self, task, data_ingestor):
        """Calculate the best 5 states based on the mean of the data values and question."""
        df = data_ingestor.data_list
        question = task.question
        df_filtered = df[(df['Question'] == question) &
            (df['YearStart'] >= 2011) &
            (df['YearEnd'] <= 2022)]
        averages = df_filtered.groupby('LocationDesc')['Data_Value'].mean()
        if question in data_ingestor.questions_best_is_max:
            averages_sorted = averages.sort_values(ascending=False).to_dict()
        else:
            averages_sorted = averages.sort_values().to_dict()
        best5 = dict(list(averages_sorted.items())[:5])
        best5_json = json.dumps(best5)
        self.logger.info(f"Best 5 states were calculated for {task.job_id}")
        self.write_result(best5_json, task.job_id)

    def worst5(self, task, data_ingestor):
        """Calculate the worst 5 states based on the mean of the data values and question."""
        df = data_ingestor.data_list
        question = task.question
        df_filtered = df[(df['Question'] == question) &
            (df['YearStart'] >= 2011) &
            (df['YearEnd'] <= 2022)]
        averages = df_filtered.groupby('LocationDesc')['Data_Value'].mean()
        if question in data_ingestor.questions_best_is_min:
            averages_sorted = averages.sort_values(ascending=False).to_dict()
        else:
            averages_sorted = averages.sort_values().to_dict()
        worst5 = dict(list(averages_sorted.items())[:5])
        worst5_json = json.dumps(worst5)
        self.logger.info(f"Worst 5 states were calculated for {task.job_id}")
        self.write_result(worst5_json, task.job_id)

    def global_mean(self, task, df):
        """Calculate the global mean of the data values for a specific question."""
        question = task.question
        df_filtered = df[(df['Question'] == question) &
            (df['YearStart'] >= 2011) &
            (df['YearEnd'] <= 2022)]
        global_mean = df_filtered['Data_Value'].mean()
        global_mean_json = json.dumps({"global_mean": global_mean})
        self.logger.info(f"Global mean was calculated for {task.job_id}")
        self.write_result(global_mean_json, task.job_id)

    def diff_from_mean(self, task, df):
        """Calculate the difference of the mean of the data values 
            for each state from the global mean."""
        question = task.question
        df_filtered = df[(df['Question'] == question) &
            (df['YearStart'] >= 2011) &
            (df['YearEnd'] <= 2022)]
        global_mean = df_filtered['Data_Value'].mean()
        averages = df_filtered.groupby('LocationDesc')['Data_Value'].mean().sort_values().to_dict()
        diff_from_mean = {location: global_mean - averages[location] for location in averages}
        diff_from_mean_json = json.dumps(diff_from_mean)
        self.logger.info(f"Difference from mean was calculated for {task.job_id}")
        self.write_result(diff_from_mean_json, task.job_id)

    def state_mean_by_category(self, task, df):
        """Calculate the mean of the data values for a specific state by categpries."""
        df_filtered = df[(df['Question'] == task.question)
                         & (df['LocationDesc'] == task.state)].copy()
        grouped = df_filtered.groupby(['StratificationCategory1', 'Stratification1'])['Data_Value']
        grouped_dict = grouped.mean().to_dict()
        averages = {str(key): grouped_dict[key] for key in grouped_dict}
        averages_json = json.dumps({task.state : averages})
        self.logger.info(f"State mean by categories was calculated for {task.job_id}, for {task.state}")
        self.write_result(averages_json, task.job_id)

    def state_diff_from_mean(self, task, df):
        """Calculate the difference of the mean of the data values 
        for a specific state from the global mean."""
        question = task.question
        df_filtered = df[(df['Question'] == question) &
            (df['YearStart'] >= 2011) &
            (df['YearEnd'] <= 2022)]
        global_mean = df_filtered['Data_Value'].mean()
        df_new = df_filtered[(df['LocationDesc'] == task.state)].copy()
        # df_new = df_filtered[(df_filtered[task.state])].copy()
        averages = df_new.groupby('LocationDesc')['Data_Value'].mean()
        averages_dict = averages.to_dict()
        diff_from_mean_json = json.dumps({task.state: global_mean - averages_dict[task.state]})
        self.logger.info(f"Difference from mean was calculated for task {task.job_id}")
        self.write_result(diff_from_mean_json, task.job_id)

    def mean_by_category(self, task, df):
        """Calculate the mean of the data values 
            for a specific question by category."""
        df_new = df[df['Question'] == task.question]
        grouped = df_new.groupby(['LocationDesc',
                                  'StratificationCategory1', 'Stratification1'])['Data_Value']
        grouped_dict = grouped.mean().to_dict()
        averages = {str(key): grouped_dict[key] for key in grouped_dict}
        averages_json = json.dumps(averages)
        self.logger.info(f"Mean by category was calculated for task {task.job_id}")
        self.write_result(averages_json, task.job_id)
