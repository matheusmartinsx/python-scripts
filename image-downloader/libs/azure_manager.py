import os
import re
import csv

from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContentSettings


class AzureManager():
    '''
    Class responsible to send files to an Azure container and generate a csv file with the new access links to the those files

    Attributes
    ----------
    account : str
        Azure Account name to compose the new url
    container : str
        The target container to send the images
    connection_string : str
        The connection string to authorize access to Azure APIs

    Methods
    -------
    send_files(directory, content_type)
        Send the files to the Azure container from a given directory, setting it's content type
    _generate_csv_from_files(directory_files)
        Read the files from the origin directory and creates the new links
    '''
    def __init__(self, account: str, container: str, connection_string: str):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container)
        self.url_base = f'https://{account}.blob.core.windows.net/{container}/'
        self.root_folder = os.getcwd().replace('\\', '/')
        self.report_path = f'{self.root_folder}/report'
        if not self.container_client.exists():
            print(f'Container {container} not found!')
            return False

    def send_files(self, directory: str = 'images', content_type: str = 'image/jpeg'):
        '''
        Send the files from the given directory to the set Azure container

        Parameters
        ----------
        directory : str
            The origin directory from where the files will be read
        content_type : str
            The content type for the files that will be sent to the container
        '''
        folder = f'{self.root_folder}/{directory}'
        files_to_upload = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        self._generate_csv_from_files(files_to_upload)
        content_settings = ContentSettings(content_type=content_type)
        count = 0
        for file in files_to_upload:
            with open(f'{folder}/{file}', "rb") as f:
                self.container_client.upload_blob(name=f'{file}', data=f,
                                                  overwrite=True, content_settings=content_settings)
                print(f'Image [ {file} ] upload completed. - {count}/{len(files_to_upload)}')
                count += 1

    def _generate_csv_from_files(self, directory_files: list):
        '''
        Read the files from the origin directory, groups the files with the same base and creates the new access links

        Parameters
        ----------
        directory_files : list
            A list with all the files contained in a directory
        '''
        dict_files = {}
        for file in directory_files:
            file_name = file
            end_string = '[/_][0-9].jpg'
            ends_with = re.search(end_string, file_name)
            if ends_with is not None:
                sufix = file_name.split('_')[-1]
                base_name = file_name.replace(f'_{sufix}', '')
                try:
                    know_files = dict_files[base_name]
                    know_files.append(file_name)
                    dict_files[base_name] = know_files
                except KeyError:
                    dict_files[base_name] = [file_name]
            else:
                base_name = file_name.split('.')[0]
                try:
                    know_files = dict_files[base_name]
                    know_files.append(file_name)
                    dict_files[base_name] = know_files
                except KeyError:
                    dict_files[base_name] = [file_name]
        for file_name in dict_files:
            file_list = dict_files[file_name]
            urls = [file_name]
            for name in file_list:
                urls.append(f'{self.url_base}{name}')
            with open(f'{self.report_path}/new_file.csv', 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(urls)
