from libs.image_manager import ImageManager
from libs.azure_manager import AzureManager

def main():
    csv_file_name = input('Insert your CSV file name: ')
    if csv_file_name.endswith('.csv'):
        pass
    else:
        csv_file_name += '.csv'
    image_prefix = input('Insert your desired image name Prefix: ')
    azure_account = input('Insert your Azure Account Name: ')
    azure_container = input('Insert your azure container name: ')
    connection_string = '' #Insert your Connection String

    img_manager = ImageManager(image_prefix, True)
    downloaded = img_manager.start_download(csv_path=csv_file_name)
    print(f'{downloaded} - Files Downloaded')
    azure_manager = AzureManager(
        account=azure_account,
        container=azure_container,
        connection_string=connection_string
    )
    azure_manager.send_files(directory='images')


if __name__ == "__main__":
    main()
