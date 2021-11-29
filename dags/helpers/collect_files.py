import os
from airflow.models import Variable

def collect_files(ftp):
    """
    Collects all the files in under the 'ftp_folder' folder.
    Files starting with a dot (.) are omitted.
    :ftp FTPHost:
    :return: list of all found file's path
    """

    # make sure you're in root dir
    ftp.chdir('/')
    result = []
    ftp_folder = "dummy_test_dir"
    for path, dirs, files in ftp.walk(ftp_folder):
        for filename in files:
            if filename.startswith('.'):
                continue
            full_path = os.path.join(path, filename)
            if filename.endswith('.zip') or filename == 'go.xml':
                result.append(full_path)
            else:
                print(f'File with invalid extension on FTP path={full_path}')
                
    Variable.set("downloaded_files_names", result)
    return result


