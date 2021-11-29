from airflow.models import Variable

import zipfile
import os

from helpers.create_dir import create_dir


def unzip_files(value, raw_file_name):
    cwd = os.getcwd()
    print(value)

    zipFile_obj = zipfile.ZipFile(value)
    zipped_files_names=zipFile_obj.namelist()
    print(zipped_files_names)

    zip_file_obj = {'zipped_folder_name': value, 'files_inside':  zipped_files_names}
    Variable.set(raw_file_name, zip_file_obj) # value yra root folderis, isk ur visi zipai pareina
    print(zip_file_obj)
    paths_for_unzipped_files = []
    file_name = os.path.basename(value)
    grouping_folder = file_name.split(".zip")[0]
    path_for_unzipped_files = os.path.join(
          cwd, "unzipped", grouping_folder)
    with zipfile.ZipFile(value, 'r') as zip_ref:
            try:
                zip_ref.extractall(path_for_unzipped_files)
                print(f'file {value} is extracted successfully')
                paths_for_unzipped_files.append(path_for_unzipped_files)
            except Exception as e:
                print(
                    f'Error while extracting the file {value}: {e}')
    return zipped_files_names
