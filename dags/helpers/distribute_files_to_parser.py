import os 
from helpers.constants import UNZIPPED_FILES_FOLDER

def distribute_files_to_parser(file_name):
    cwd = os.getcwd()
    path = os.path.join(cwd, UNZIPPED_FILES_FOLDER)
    try:
        os.chdir(path)
        unzipped_files = []
        for path, dirs, files in os.walk(file_name):
            unzipped_files.append(files)
        return unzipped_files
    except Exception as e:
        return []

