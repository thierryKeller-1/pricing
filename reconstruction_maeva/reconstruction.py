import os
import json
import constants
import pandas as pd

from typing import Literal
from pathlib import Path
from datetime import datetime, timedelta



class SITE:
    MAEVA:str = "maeva"
    BOOKING:str = "booking"
    EDOMIZIL:str = "edomizil"



class Datarecover(object):

    def __init__(self, site:Literal["SITE.MAEVA", "SITE.BOOKING", "SITE.EDOMIZIL"]) -> None:
        self.site = site
        self.log = {}

        self.output_path = f"{constants.OUTPUT_DIR}/{self.site}/missing_{self.site}.csv"
        self.log_file_path = f"{constants.LOG_FILE_DIR}/{self.site}/log.json"
        self.file_to_check = ""
        self.file_checker = ""

    def is_path_exit(self, path:str) -> bool:
        """## check if any path exist or not
        ### Args:
            - `path`: file path
        ### Returns:
            - `bool`: true if exist else false
        """
        return Path(path).exists()

    def read_logfile(self) -> None:
        """## read log file if exist"""
        if self.is_path_exit(self.log_file_path):
            with open(self.log_file_path, mode='r', encoding='utf-8') as openfile:
                self.log = json.load(openfile)
        else:
            self.create_log()

    def create_log(self) -> None:
        """## create a log file to store data and allow continue if error raised"""
        files = self.get_ordered_files_path()
        default_log = {}
        default_log['files'] = files
        default_log['total_files'] = len(files)
        default_log['file_checker'] = files[0]
        default_log['file_to_check'] = files[1]
        default_log['last_file_index'] = 1
        default_log['last_row_index'] = 0
        if not self.is_path_exit(self.log_file_path):
            self.set_log(log=default_log)

    def get_log(self, key:str) -> object | None:
        try:
            return self.log[key]
        except KeyError:
            return None

    def set_log(self, log:dict,key:str=None, Key_value:object=None) -> None:
        """## update log file
        ### Args:
            - `log (dict)`: log data. it update all log if no key or Key_value.
            - `key (str, optional)`: key of log to be update. Defaults to None.
            - `Key_value (object, optional)`: log value to asign with key. Defaults to None.
        """
        if key and Key_value:
            self.log[key] = Key_value
        else:
            self.log = log
            with open(self.log_file_path, 'w', encoding='utf-8') as openfile:
                openfile.write(json.dumps(self.log))

    def create_file(self) -> None:
        """ ## create file to save missing data """
        if not self.is_path_exit(self.output_path):
            os.makedirs(f"{constants.OUTPUT_DIR}/{self.site}/")
            df = pd.DataFrame(columns=constants.FIELDS[self.site])
            df.to_csv(self.output_path, index=False)

    def save_missing_data(self, values:list) -> None:
        """ ## save data to csv
        ### Args:
            - `values (list)`: list of data dictionary format
        """
        df = pd.DataFrame(values)
        df.to_csv(self.output_path, index=False, mode='a')

    def search_at_dataframe(self, dataframe:object, data_to_find:dict, check_fields:list) -> tuple:
        """## search data type dictionary in dataframe
        ### Args:
            - `dataframe (object)`: dataframe
            - `data_to_find (dict)`: data to find if already exist on dataframe or not
            - `check_fields (list)`: dataframe column to be used to find data 
        ### Returns:
            - `tuple`: return true if result is not empty and data result search
        """
        conditions = pd.Series([True] * len(dataframe)) 
        for check_field in check_fields:
            conditions &= (dataframe[check_field] == data_to_find[check_field])
        result = dataframe[conditions]
        print(f"  ==>  {len(result)} data found")
        return not result.empty, result

    def read_file(self, csv_file_path:str) -> pd.DataFrame | None:
        """## read csv files
        ### Args:
            - `csv_file_path (str)`: path to the csv file to be read
        ### Returns:
            - `pd.DataFrame | None`: return a pandas dataframe if file path exist else None
        """
        try:
            return pd.read_csv(csv_file_path)
        except:
            return None
        
    def get_ordered_files_path(self) -> list:
        """## get csv files path
        ### Returns:
            - `list`: list contains path of file 
        """
        # List to hold file paths with corresponding dates
        files_with_dates = []

        # Walk through the root directory
        for dirpath, dirnames, filenames in os.walk(constants.BASE_DIR):
            try:
                dir_date = datetime.strptime(os.path.basename(dirpath), '%d-%m-%Y')
            except ValueError:
                continue 

            # Find all files that match the pattern
            for filename in filenames:
                if (filename.startswith(self.site)) and filename.endswith('.csv'):
                    files_with_dates.append((os.path.join(dirpath, filename), dir_date))

        # Sort the list of files by the date part
        files_with_dates.sort(key=lambda x: x[1])
        print('Ordered Files :',files_with_dates)
        # Extract and return only the file paths in sorted order
        sorted_file_paths = [file_path for file_path, _ in files_with_dates]
        return sorted_file_paths

    def print_treatment_range(self) -> int:
        df = self.read_file(self.file_checker)
        current_index = self.get_log('last_row_index')
        percent = (current_index / len(df)) * 100
        print(f"  ==> {round(percent, 2)} %")
    
    def check_data_if_valid(self, data_to_check:dict) -> bool:
        """## check curent data if all column is valid

        ### Args:
            - `data_to_check (dict)`: data row

        ### Returns:
            - `bool`: return true if valid else false
        """

    

    def run(self) -> None:
        self.create_log()
        self.read_logfile()

        for i in range(self.log['last_file_index'], self.log['total_files']):
            print("  ==> Loading files")
            if self.log['last_file_index'] <= 1:
                self.file_checker = self.read_file(self.log['file_checker'])
            else:
                self.file_checker = self.read_file(self.output_path)
            self.file_to_check = self.read_file(self.log['file_to_check'])
            for k in range(len(self.file_to_check)):
                data_to_check = self.file_to_check.iloc[k].to_dict()
                if self.check_data_if_valid(data_to_check):
                    has_missing_data, search_result = self.search_at_dataframe(
                                                    dataframe=self.file_checker, 
                                                    data_to_find=data_to_check, 
                                                    check_fields=constants.CHEKING_FIELDS[self.site])
                    if has_missing_data:
                        pass










if __name__ == "__main__":
    d = Datarecover(site=SITE.MAEVA)
    