import os
from scrapy.selector import Selector
from hepcrawl.spiders.oup_spider import OxfordUniversityPressSpider
import json

from helpers.create_dir import create_dir
from helpers.constants import DOWNLOADED_FILES_FROM_S3

# Don't foget to set export SCOAP_DEFAULT_LOCATION and HEPCRAWL_BASE_WORKING_DIR  to JSONS folder
class Test:
    meta = {
        "pdf_url": ''}


def crawler_parser(key_in_s3):

    """ crawler_parser takes xml files from downloaded_from_s3 folder and parses to JSON format. JSONS is written to
    the files in folder JSONS.
    Files are grouped by the folder name, that they were extracted from.
    key_in_s3 reflects the folder and the name of the file.
    """
    print('called!!!!', key_in_s3)
    file_name = os.path.basename(key_in_s3)
    cwd = os.getcwd()
    # # grouping folder is the zipped folder name, which we downloaded from FTP
    grouping_folder = os.path.basename(os.path.dirname(key_in_s3))
    # path_to_s3_folder = os.path.join(cwd, 'downloaded_from_s3', grouping_folder)
    # jsons_dir - dir where parsed JSONS will be saved
    JSONS = 'jsons'
    jsons_dir = create_dir(cwd, JSONS)

    # file_full_path = os.path.join(path_to_s3_folder, file_name)
    suffix = file_name.split('.')[-1]
    file_full_path = os.path.join(cwd, DOWNLOADED_FILES_FROM_S3, grouping_folder)

    if jsons_dir and suffix == 'xml':
        is_the_grouping_folder_created_in_jsons_dir = create_dir(cwd, JSONS, grouping_folder)
        jsons_dir_path = os.path.join(cwd, JSONS, grouping_folder)
        if is_the_grouping_folder_created_in_jsons_dir:
            try:
                with open(file_full_path, 'r') as file:
                    selector = Selector(text=file.read(), type=suffix)
                    spider = OxfordUniversityPressSpider(target_folder=os.path.join(cwd, JSONS))
                    try:  # if xml file is corrupted
                        json_obj = spider.parse_node(Test(), selector)
                        file_name_with_json_suffix = file_name.replace(suffix, 'json')
                        jsons_full_path = os.path.join(jsons_dir_path, file_name_with_json_suffix)
                        with open(jsons_full_path, 'w') as json_file:
                            parsed = json.loads(str(json_obj).replace("'", '"'))
                            json_file.write(json.dumps(parsed, indent=4, sort_keys=True))
                    except Exception as e:
                        print(f'ERROR while parsing a file {file_full_path}: {e}')
            except Exception as e:
                print(f'ERROR while opening  a file {file_full_path}: {e}')
