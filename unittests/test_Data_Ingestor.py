import unittest
import sys
sys.path.append('app')
from app.data_ingestor import DataIngestor

class TestDataIngestor(unittest.TestCase):
    def test_csv_parsing(self):
        csv_path = "./nutrition_activity_obesity_usa_subset.csv"  # Înlocuiește cu calea către fișierul tău CSV de test
        data_ingestor = DataIngestor(csv_path)
        data_list = data_ingestor.data_list

        question = 'Percent of adults who report consuming vegetables less than one time daily'

        expectedValue =['29.7', '19.3', '21.3', '15.0', '20.7', '19.3', '19.8', '14.6']
        res_list = []
        for entry in data_list:
            if entry['LocationDesc'] == 'California' and entry['YearStart'] == '2021'  and entry['YearEnd'] == '2021' and entry['Question'] == question:
                res_list.append(entry['Data_Value'])
        self.assertEqual(res_list, expectedValue)

if __name__ == '__main__':
    unittest.main()