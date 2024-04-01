import unittest
import sys
sys.path.append('app')
from app.data_ingestor import DataIngestor
import pandas as pd

class TestDataIngestor(unittest.TestCase):
    def test_csv_parsing(self):
        csv_path = "./nutrition_activity_obesity_usa_subset.csv" 
        data_ingestor = DataIngestor(csv_path)
        data_list = data_ingestor.data_list

        question = 'Percent of adults who report consuming vegetables less than one time daily'

        expectedValue =[29.7, 19.3, 21.3, 15.0, 20.7, 19.3, 19.8, 14.6]
        res_list = []
        df = pd.DataFrame(data_list)
        df_filtered = df[(df['Question'] == question) & (df['LocationDesc'] == 'California') & (df['YearStart'] == 2021) & (df['YearEnd'] == 2021)]
        res_list = df_filtered['Data_Value'].to_list()

        self.assertEqual(res_list, expectedValue)

if __name__ == '__main__':
    unittest.main()