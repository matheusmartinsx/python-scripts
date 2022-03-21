import urllib3
import socket
import requests
import csv
import os
import PIL

from PIL import Image, UnidentifiedImageError
from urllib.error import HTTPError
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor



class ImageManager():
    '''
    Class responsible to download and resize images from a given csv file

    Attributes
    ----------
    image_name_prefix : str
        Prefix to insert in the images names
    resize : bool
        Flag to indicate if the images will be resized
    new_size : int
        Max width or height size to the new image

    Methods
    -------
    start_download()
        Start the download process with a Thread Pool 
    _donwload_images(link_list) -> bool
        Receives a list with a file name and a link and return if the download was succesfull
    _return_links(csv_path) -> list
        Receives a csv file path and return a list of file names and links
    _remove_transparency(image_object, background_color) -> PIL.Image.Image
        Receives a PIL object, removes all transparencys and returns the new object
    _resize_image(image) -> PIL.Image.Image
        Receives a PIL object, resizes it the the desired size and returns the new object
    '''
    def __init__(self, image_name_prefix: str = '', resize: bool = False, new_size: int = 1000):
        self.name_prefix = image_name_prefix
        self.resize = resize
        self.new_size = new_size
        self.image_path = str(os.getcwd().replace('\\', '/')) + '/images'
        self.error_path = str(os.getcwd().replace('\\', '/')) + '/report'
        try:
            os.mkdir(self.image_path)
        except FileExistsError:
            pass
        try:
            os.mkdir(self.error_path)
        except FileExistsError:
            pass

    def start_download(self, csv_path: str) -> int:
        download_links = self._return_links(csv_path=csv_path)
        total_downloaded = 0
        with ThreadPoolExecutor() as executor:
            for url_pair in download_links:
                future = executor.submit(self._donwload_images, url_pair)
                result = future.result()
                if result:
                    total_downloaded += 1
        return total_downloaded

    def _donwload_images(self, link_list: list) -> bool:
        '''
        Download an image file from a given image name and url pair list

        Parameters
        ----------
        link_list : list
            A list with a image name and url pair

        Returns
        -------
        bool
            Returns True if the download is succesfull and False otherwise
        '''
        hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'}
        img_name = f'{self.name_prefix}{link_list[0]}.jpg'
        link = link_list[1]
        try:
            timeout = urllib3.util.Timeout(connect=5, read=10)
            http = urllib3.PoolManager(timeout=timeout)
            response = http.request('GET', link, headers=hdr)
            if response.status != 200:
                raise HTTPError(link, response.status,  response.reason, response.headers, None)
            file = Image.open(BytesIO(response.data))
            file = self._remove_transparency(file)
            file = file.convert('RGB')
            if self.resize:
                new_image = self._resize_image(file)
                new_image.save(f'{self.image_path}/{img_name}')
                print(f"\tImage {img_name} resized")
                return True
            else:
                file.save(f'{self.image_path}/{img_name}')
                print(f"\tImage {img_name} resized")
                return True
        except UnicodeEncodeError:
            response = requests.get(link, timeout=10)
            file = Image.open(BytesIO(response.content))
            file = self._remove_transparency(file)
            file = file.convert('RGB')
            if self.resize:
                new_image = self._resize_image(file)
                new_image.save(f'{self.image_path}/{img_name}')
                print(f"\tImage {img_name} resized")
                return True
            else:
                file.save(f'{self.image_path}/{img_name}')
                print(f"\tImage {img_name} resized")
                return True
        except UnidentifiedImageError:
            with open(f'{self.error_path}/error.csv', 'a', newline='', encoding='UTF-8') as f:
                writer = csv.writer(f)
                writer.writerow([img_name, 'Image Broken', link])
            print('Error downloading image: Image Broken')
            return False
        except HTTPError as err:
            with open(f'{self.error_path}/error.csv', 'a', newline='', encoding='UTF-8') as f:
                writer = csv.writer(f)
                writer.writerow([img_name, str(err), link])
            print(f'Error downloading image: {err}')
            return False
        except socket.timeout:
            with open(f'{self.error_path}/error.csv', 'a', newline='', encoding='UTF-8') as f:
                writer = csv.writer(f)
                writer.writerow([img_name, 'Timeout Err', link])
            print('Error downloading image: Connection Timeout')
            return True

    def _return_links(self, csv_path: str) -> list:
        '''
        Read a csv file from the the given path and return a list of lists

        Parameters
        ----------
        csv_path : str
            A string containing the path to the base csv file

        Returns
        -------
        list
            Returns a list of sublists, containing a image name and url pair
        '''
        with open(csv_path, 'r') as f:
            url_list = list(csv.reader(f))
        link_list = []
        for row in url_list:
            img_links = []
            img_row_name = row[0]
            for columns in row:
                if 'http' in columns:
                    img_links.append(columns)
                else:
                    continue
            count = 0
            img_name = ''
            num = ''
            for link in img_links:
                if count > 0:
                    num = '_' + str(count)
                img_name = img_row_name + num
                link_list.append([img_name, link])
                count += 1
        return link_list

    def _remove_transparency(self, image_object: PIL.Image.Image, background_color=(255, 255, 255)) -> PIL.Image.Image:
        '''
        Remove all the transparency layers from a given PIL object

        Parameters
        ----------
        image_object : PIL.Image.Image
            A PIL Image Object to have it's transparencies removed
        background_color : tuple
            A tuple containing the RGB value of the background color to be used

        Returns
        -------
        PIL.Image.Image
            Returns a PIL Image Object with it's transparencies removed
        '''
        if image_object.mode in ('RGBA', 'LA', 'P') or (image_object.mode == 'P' and 'transparency' in image_object.info):
            alpha = image_object.convert('RGBA').split()[-1]
            background = Image.new("RGBA", image_object.size, background_color + (255,))
            background.paste(image_object, mask=alpha)
            return background
        else:
            return image_object

    def _resize_image(self, image: PIL.Image.Image) -> PIL.Image.Image:
        '''
        Receive, resize and returns a PIL Object

        Parameters
        ----------
        image : PIL.Image.Image
            A PIL Image Object to be resized

        Returns
        -------
        PIL.Image.Image
            Returns a PIL Image Object with it's new size
        '''
        width, height = image.size
        if width != height:
            size = (self.new_size, self.new_size)
            if width > height:
                wpercent = (self.new_size/width)
                hsize = int(height*float(wpercent))
                image = image.resize((self.new_size, hsize), PIL.Image.LANCZOS)
                new_image = Image.new('RGB', size, (255, 255, 255))
                new_image.paste(image, (int((size[0] - image.size[0]) / 2), int((size[1] - image.size[1]) / 2)))
                return new_image
            elif height > width:
                hpercent = (self.new_size/height)
                wsize = int(width*float(hpercent))
                image = image.resize((wsize, self.new_size), PIL.Image.LANCZOS)
                new_image = Image.new('RGB', size, (255, 255, 255))
                new_image.paste(image, (int((size[0] - image.size[0]) / 2), int((size[1] - image.size[1]) / 2)))
                return new_image
            else:
                image = image.resize(size, PIL.Image.LANCZOS)
                return image
        elif width != self.new_size or height != self.new_size:
            image = image.resize((self.new_size, self.new_size), PIL.Image.LANCZOS)
            return image
        else:
            return image
