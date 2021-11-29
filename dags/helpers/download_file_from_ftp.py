import os
from time import localtime, strftime

from helpers.create_dir import create_dir
from helpers.constants import DOWNLOADED_FILES_FOLDER
from airflow.models import Variable

def download_file_from_ftp(ftp, file_name):
    # come back to root dir, because file which will be downloaded have absolute path
    ftp.chdir('/')
    # where downloaded files will be saved
    create_dir(os.getcwd(), DOWNLOADED_FILES_FOLDER)
    target_folder = os.path.abspath(os.path.join(os.getcwd(), DOWNLOADED_FILES_FOLDER))
    filename_prefix = strftime('%Y-%m-%d_%H:%M:%S', localtime())
    new_file_name = '%s_%s' % (filename_prefix, os.path.basename(file_name))
    local_filename = os.path.join(target_folder, new_file_name)

    dir_name = os.path.dirname(file_name)
    ftp.download_if_newer(file_name, local_filename)
    print(f'File {new_file_name} with path {dir_name} to {target_folder}')
    Variable.set("downloaded_file", local_filename)

    return local_filename


    