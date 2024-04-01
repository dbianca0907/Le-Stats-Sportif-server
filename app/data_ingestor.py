import os
import json
import csv
import pandas as pd

class DataIngestor:
    def __init__(self, csv_path: str):
        # TODO: Read csv from csv_path
        # df = pd.read_csv(csv_path, dtype={'YearStart': int, 'YearEnd': int, 'Data_Value': float})
        # df = pd.read_csv(csv_path)

        # selected_columns = [
        #     'YearStart', 'YearEnd', 'LocationDesc', 'Question',
        #     'StratificationCategory1', 'Stratification1', 'Data_Value'
        # ]
        # df = df[selected_columns]
        # # do a dict list which will have as a key (Question, YearStart, YearEnd, LocationDesc) and as values all the data values for that key
        # self.data_list = []
        # for index, row in df.iterrows():
        #     key = (row['Question'], row['YearStart'], row['YearEnd'], row['LocationDesc'])
        #     value = row['Data_Value']
        #     self.data_list.append((key, value))
        
        # self.data_list_category = []
        # for index, row in df.iterrows():
        #     key = (row['Question'], row['YearStart'], row['YearEnd'], row['LocationDesc'], row['StratificationCategory1'], row['Stratification1'])
        #     value = row['Data_Value']
        #     self.data_list_category.append((key, value))

        data_list = []
        with open(csv_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data_entry = {
                    'LocationDesc': row['LocationDesc'],
                    'Question': row['Question'],
                    'YearStart': int(row['YearStart']),
                    'YearEnd': int(row['YearEnd']),
                    'StratificationCategory1': row['StratificationCategory1'],
                    'Stratification1': row['Stratification1'],
                    'Data_Value': float(row['Data_Value'])
                }
                data_list.append(data_entry)

        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]
        self.data_list = data_list
