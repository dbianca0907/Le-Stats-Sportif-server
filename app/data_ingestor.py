import os
import json
import csv

class DataIngestor:
    def __init__(self, csv_path: str):
        # TODO: Read csv from csv_path
        # data_dict = {}
        # with open(csv_path, 'r') as file:
        #     reader = csv.DictReader(file)
        #     for row in reader:
        #         location_abbr = row['LocationAbbr']
        #         year_start = row['YearStart']
        #         year_end = row['YearEnd']
        #         question = row['Question']
        #         data_value = row['Data_Value']
        #         if location_abbr not in data_dict:
        #             data_dict[location_abbr] = {}

        #         if year_start not in data_dict[location_abbr]:
        #             data_dict[location_abbr][year_start] = {}
                
        #         if year_end not in data_dict[location_abbr][year_start]:
        #             data_dict[location_abbr][year_start][year_end] = {}

        #         if question not in data_dict[location_abbr][year_start][year_end]:
        #             data_dict[location_abbr][year_start][year_end][question] = []

        #         data_dict[location_abbr][year_start][year_end][question].append(data_value)
        data_list = []
        with open(csv_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                data_entry = {
                    'YearStart': row['YearStart'],
                    'YearEnd': row['YearEnd'],
                    'LocationDesc': row['LocationDesc'],
                    'Question': row['Question'],
                    'StratificationCategory1': row['StratificationCategory1'],
                    'Stratification1': row['Stratification1'],
                    'Data_Value': row['Data_Value']
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
