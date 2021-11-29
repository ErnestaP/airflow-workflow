import ftputil
import os

from dotenv import load_dotenv

from helpers.ftp_connection import ftp_session_factory


def connect_to_ftp():
    load_dotenv()

    ftp_host_name = os.getenv('FTP_HOST')
    ftp_user = os.getenv('FTP_USER')
    ftp_password = os.getenv('FTP_PASSWORD')

    ftp_host = ftputil.FTPHost(ftp_host_name,
                               ftp_user,
                               ftp_password,
                               session_factory=ftp_session_factory)

    ftp_host.use_list_a_option = False
    ftp_host.chdir('dummy_test_dir')
    path = ftp_host.getcwd()
    
    if 'dummy_test_dir' in path:
        ftp_host.upload_if_newer('oup.xml.zip', 'oup.xml.zip')
        ftp_host.upload_if_newer('scoap3.archival.zip', 'scoap3.archival.zip')
        ftp_host.upload_if_newer('scoap3.pdf.zip', 'scoap3.pdf.zip')
    return ftp_host
