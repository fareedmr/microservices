import csv
from datetime import datetime
from pymongo import MongoClient

# define column headers manually since the input file doesn't have them
COLUMNS = ['FirstName', 'LastName', 'Company', 'BirthDate', 'Salary', 'Address',
           'Suburb', 'State', 'Post', 'Phone', 'Mobile', 'Email']

# Address class
class Address:

    def __init__(self, address, suburb, state, post):

        self.address = address.strip()
        self.suburb = suburb.strip()
        self.state = state.strip()
        self.post = int(post)

    def to_dict(self):

        return {
            'address': self.address,
            'suburb': self.suburb,
            'state': self.state,
            'post': self.post,
        }

# Employee class
class Employee:

    def __init__(self, data):

        self.FullName = f"{data['FirstName'].strip()} {data['LastName'].strip()}"

        self.Company = data['Company'].strip()

        # parse the BirthDate and handle potential errors
        birthdate_str = data['BirthDate'].strip()
        birthdate_str = self.validate_and_correct_date(birthdate_str)

        # default to None, we'll check if `birthdate_str` is valid and update this value accordingly
        self.BirthDate = None

        if birthdate_str:
            try:
                self.BirthDate = datetime.strptime(birthdate_str, '%d%m%Y').strftime('%d-%m-%Y')

            except ValueError as e:
                self.BirthDate = None

        self.Salary = f"${float(data['Salary']):,.2f}"

        self.Age = self.calculate_age(self.BirthDate)

        self.SalaryBucket = self.assign_salary_bucket(float(data['Salary']))

        self.Address = Address(data['Address'], data['Suburb'], data['State'], data['Post'])

        self.Phone = data['Phone'].strip()
        self.Mobile = data['Mobile'].strip()
        self.Email = data['Email'].strip()

    @staticmethod
    def validate_and_correct_date(date_str):

        # remove leading/trailing spaces
        date_str = date_str.strip()

        # add a leading zero if the length of the date string is 7
        if len(date_str) == 7:
            date_str = '0' + date_str

        # check if the date string is still less than 8 characters after correction
        if len(date_str) != 8:
            return ''

        return date_str

    @staticmethod
    def calculate_age(birthdate):

        # checking if birthdate is valid
        if birthdate is None:
            return None

        reference_date = datetime(2024, 3, 1)
        birth_date = datetime.strptime(birthdate, '%d-%m-%Y')
        age = reference_date.year - birth_date.year

        if reference_date < birth_date.replace(year=reference_date.year):
            age -= 1

        return age

    @staticmethod
    def assign_salary_bucket(salary):

        if salary < 50000:
            return "A"
        elif 50000 <= salary <= 100000:
            return "B"
        else:
            return "C"

    def to_dict(self):

        return {
            'FullName': self.FullName,
            'Company': self.Company,
            'BirthDate': self.BirthDate,
            'Salary': self.Salary,
            'Age': self.Age,
            'SalaryBucket': self.SalaryBucket,

            # adding Address as a nested dictionary object
            'Address': self.Address.to_dict(),

            'Phone': self.Phone,
            'Mobile': self.Mobile,
            'Email': self.Email,
        }

# Extract: Read pipe-separated data from the data file
def extract_data(file_path):

    with open(file_path, 'r') as file:

        reader = csv.DictReader(file, delimiter='|', fieldnames=COLUMNS)
        rows = [row for row in reader]

    return rows

# Transform: Clean, format, and transform the extracted data
def transform(data):

    return [Employee(row).to_dict() for row in data]

# Load: Insert transformed data into MongoDB
def load(data):

    # MongoDB service is named `db_service` and should be accessible on port 27017
    client = MongoClient('mongodb://db_service:27017/')

    db = client.etl_db
    collection = db.employees
    collection.insert_many(data)
    client.close()

if __name__ == "__main__":

    # Data file to be processed
    raw_data = extract_data('/data/member-data.txt')

    transformed_data = transform(raw_data)

    load(transformed_data)

    print("Data successfully loaded into MongoDB.")
