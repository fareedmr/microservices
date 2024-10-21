# import unittest
# from etl_pipeline import extract_data, transform
#
# class TestETL(unittest.TestCase):
#
#     def test_extract(self):
#
#         data = extract_data('/data/member-data.txt')
#         self.assertEqual(len(data), 500)
#
#     def test_transform(self):
#         raw_data = [
#             {
#                 'FirstName': 'John',
#                 'LastName': 'Doe',
#                 'Company': 'Company Inc.',
#                 'BirthDate': '01051985',
#                 'Salary': '55000',
#                 'Address': '123 Elm St',
#                 'Suburb': 'Elmwood',
#                 'State': 'CA',
#                 'Post': '90210',
#                 'Phone': '123456789',
#                 'Mobile': '987654321',
#                 'Email': 'john.doe@example.com'
#             }
#         ]
#
#         transformed_data = transform(raw_data)
#
#         self.assertEqual(transformed_data[0]['FullName'], 'John Doe')
#         self.assertEqual(transformed_data[0]['Age'], 38)
#         self.assertEqual(transformed_data[0]['SalaryBucket'], 'B')
#
# if __name__ == '__main__':
#
#     unittest.main()


import unittest
from pymongo import MongoClient
from etl_pipeline import extract_data, transform, load
from unittest.mock import patch

class TestETL(unittest.TestCase):

    # test data extraction
    def test_extract(self):

        data = extract_data('/data/member-data.txt')
        self.assertEqual(len(data), 500)

    # test transformation logic
    def test_transform(self):

        raw_data = [
            {
                'FirstName': 'John',
                'LastName': 'Doe',
                'Company': 'Company Inc.',
                'BirthDate': '01051985',
                'Salary': '55000',
                'Address': '123 Elm St',
                'Suburb': 'Elmwood',
                'State': 'CA',
                'Post': '90210',
                'Phone': '123456789',
                'Mobile': '987654321',
                'Email': 'john.doe@example.com'
            }
        ]

        transformed_data = transform(raw_data)

        # test basic transformation
        self.assertEqual(transformed_data[0]['FullName'], 'John Doe')
        self.assertEqual(transformed_data[0]['Age'], 38)
        self.assertEqual(transformed_data[0]['SalaryBucket'], 'B')

        # test formatted salary
        self.assertEqual(transformed_data[0]['Salary'], '$55,000.00')

    # test transformation with missing data (e.g., BirthDate)
    def test_transform_missing_birthdate(self):

        raw_data = [
            {
                'FirstName': 'Jane',
                'LastName': 'Smith',
                'Company': 'Another Co.',
                'BirthDate': '',
                'Salary': '120000',
                'Address': '456 Oak St',
                'Suburb': 'Oakwood',
                'State': 'NY',
                'Post': '10001',
                'Phone': '234567890',
                'Mobile': '876543210',
                'Email': 'jane.smith@example.com'
            }
        ]

        transformed_data = transform(raw_data)

        # Check that BirthDate is None and Age is None
        self.assertIsNone(transformed_data[0]['BirthDate'])
        self.assertIsNone(transformed_data[0]['Age'])

    # test MongoDB deployment and accessibility
    def test_mongodb_connection(self):

        try:
            client = MongoClient('mongodb://db_service:27017/', serverSelectionTimeoutMS=5000)
            db = client.etl_db

            # test connection and access to the database
            self.assertIn('etl_db', client.list_database_names())

            client.close()

        except Exception as e:
            self.fail(f"MongoDB connection failed: {e}")

if __name__ == '__main__':

    unittest.main()
