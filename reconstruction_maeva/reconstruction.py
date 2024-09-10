import os
import json
import sys
import platform
import constants
import pandas as pd

from typing import Literal
from progress.bar import IncrementalBar
from pathlib import Path
from colorama import just_fix_windows_console, Fore
from datetime import datetime, timedelta


if platform.system().lower() == 'windows':
    just_fix_windows_console()


class SITE:
    MAEVA:str = "maeva"
    BOOKING:str = "booking"
    EDOMIZIL:str = "edomizil"


class Datarecover(object):

    def __init__(self, site:Literal["SITE.MAEVA", "SITE.BOOKING", "SITE.EDOMIZIL"]) -> None:
        self.site = site
        self.log = {}

        self.missing_file_path = f"{constants.OUTPUT_DIR}/{self.site}/missing_{self.site}.csv"
        self.log_file_path = f"{constants.LOG_FILE_DIR}/{self.site}/{self.site}_log.json"

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
        self.files = self.get_log('files')

    def create_log(self) -> None:
        """## create a log file to store data and allow continue if error raised"""
        files = self.get_ordered_files_path()
        default_log = {}
        default_log['files'] = files
        default_log['total_files'] = len(files)
        default_log['last_file_index'] = 0
        default_log['last_row_index'] = 0
        if not self.is_path_exit(self.log_file_path):
            with open(self.log_file_path, 'w') as openfile:
                openfile.write(json.dumps(default_log))

    def get_log(self, key:str) -> object | None:
        """## get log value
        ### Args:
            - `key (str)`: log key
        ### Returns:
            - `object | None`: log value if exist
        """
        try:
            return self.log[key]
        except KeyError:
            return None

    def set_log(self, log:dict=None,key:str=None, Key_value:object=None) -> None:
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
            openfile.write(json.dumps(self.log, indent=4))

    def create_file(self) -> None:
        """ ## create file to save missing data """
        if not self.is_path_exit(self.missing_file_path):
            try:
                os.makedirs(f"{constants.OUTPUT_DIR}/{self.site}/")
            except FileExistsError:
                pass
            df = pd.DataFrame(columns=constants.FIELDS[self.site])
            df.to_csv(self.missing_file_path, index=False)

    def save_missing_data(self, values:list) -> None:
        """ ## save data to csv
        ### Args:
            - `values (list)`: list of data dictionary format
        """
        df = pd.DataFrame(values)
        df.to_csv(self.missing_file_path, index=False,header=False, mode='a')

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
        # print(f"  ==>  {len(result)} data found")
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
        for dirpath, dirnames, filenames in os.walk(constants.BASE_DIR[self.site]):
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
        sorted_file_paths = [file_path.replace('\\', '/') for file_path, _ in files_with_dates]
        print(sorted_file_paths)
        sorted_file_paths = sorted_file_paths[::-1]
        return sorted_file_paths

    def print_treatment_range(self) -> None:
        """## show percent range of treatment """
        current_index = self.get_log('last_row_index')
        percent = (current_index / len(self.file_to_check)) * 100
        print(Fore.GREEN +f"  ==> {round(percent, 2)} %", end="\r")
        if platform.system() == 'Windows':
            os.system('cls') 
        else:
            os.system('clear')
    
    def parse_date(self, date_str:str) -> datetime:
        """## parsing date string to datetime object
        ### Args:
            - `date_str (str)`: date string
        ### Returns:
            - `datetime`: datetime object
        """
        if date_str=="" or any(c.isalpha() for c in date_str):
            return None
        date_formats = [
            '%Y-%m-%d %H:%M:%S',  
            '%Y-%m-%d',          
            '%d-%m-%Y',       
            '%d/%m/%Y'            
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

    def get_date_interval(self, data:dict) -> int:
        """## get the interval between two date
        ### Args:
            - `data (dict)`: dictinary data
        ### Returns:
            - `int`: return number of day between two date
        """
        start_date = self.parse_date(data['date_debut'])
        end_date = self.parse_date(data['date_fin'])
        return (end_date - start_date).days + 1

    def add_days(self, date_initiale_str:str, nb_days:int) -> str:
        """## update days 
        ### Args:
            - `date_initiale_str (str)`: initial date string format
            - `nb_days (int)`: number of day to be added
        ### Returns:
            - `str`: date format string
        """
        date_initiale = self.parse_date(date_initiale_str)
        nouvelle_date = date_initiale + timedelta(days=nb_days)
        return nouvelle_date.strftime('%d/%m/%Y')
    
    def update_data(self, data:dict) -> dict:
        """## update data value
        ### Args:
            - `data (dict)`: data
        ### Returns:
            - `dict`: update data value
        """
        date_interval = self.get_date_interval(data)
        data['date_price'] = self.add_days(data['date_price'], date_interval)
        data['date_debut'] = self.add_days(data['date_debut'], date_interval)
        data['date_fint'] = self.add_days(data['date_fin'], date_interval)
        data['date_debut-jour'] = data['date_debut-jour'] + 1
        return data

    def get_missing_formated_data(self, data_to_check:dict, result_list:pd.DataFrame) -> bool:
        """## check curent data if all column is valid
        ### Args:
            - `data_to_check (dict)`: data row
        ### Returns:
            - `bool`: return true if valid else false
        """
        results = []
        for k in range(len(result_list)):
            result = result_list.iloc[k].to_dict()
            if self.parse_date(result['date_debut']) == self.parse_date(data_to_check['date_debut']) and \
                self.parse_date(result['date_fin']) == self.parse_date(data_to_check['date_fin']):
                if result['prix_init'] != data_to_check['prix_init'] or result['prix_actuel'] != data_to_check['prix_actuel']:
                    new_result = self.update_data(result)
                    results.append(new_result)
        return results

    def run(self) -> None:
        self.create_log()
        self.create_file()
        self.read_logfile()
        for i in range(self.log['last_file_index'], self.log['total_files']):
            print("  ==> Loading files")
            file_index = self.get_log('last_file_index')

            try:
                self.file_checker = self.read_file(self.files[file_index])
                self.file_to_check = self.read_file(self.files[file_index + 1])

                #etape pour prendre en compte le missing file
                if file_index > 1:
                    missing_file = self.read_file(self.missing_file_path)
                    new_file = pd.concat[self.file_to_check, missing_file]
                    self.file_to_check = new_file

                bar = IncrementalBar('Processing', max=len(self.file_to_check))
                bar.index = self.log['last_row_index']
                for k in range(self.log['last_row_index'], len(self.file_to_check)):
                    data_to_check = self.file_to_check.iloc[k].to_dict()
                    has_data, search_result = self.search_at_dataframe(
                                                dataframe=self.file_checker, 
                                                data_to_find=data_to_check, 
                                                check_fields=constants.CHEKING_FIELDS[self.site])
                    if has_data:
                        pass
                        # self.save_missing_data(search_result)
                    else:
                        missing_data = self.update_data(data_to_check)
                        self.save_missing_data([missing_data])
                    self.set_log(key='last_row_index', Key_value=k+1)
                    bar.next()
                    self.print_treatment_range()
                    print(end="")
                self.set_log(key='last_file_index', Key_value=i+1)

            except IndexError:
                sys.exit()
        print("  ==> treatment done!")








if __name__ == "__main__":
    d = Datarecover(site=SITE.MAEVA)
    d.run()
    