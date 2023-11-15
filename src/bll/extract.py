# coding: utf-8
""" Data Extraction Module"""

# built-in
from time import time
from os import path as os_path
from concurrent.futures import ThreadPoolExecutor

# installed
from logging import getLogger
from wget import download as wget_download, bar_adaptive

# custom
from cnpj.config import Config
from cnpj.src.dal.file import File
from cnpj.src.bll.start import Start


class Extract:
    """
    Data extraction class.

    Attributes
    ----------
    None.

    Methods
    -------
    None.
    """

    def __init__(self):
        self.file = File()
        self.start = Start()
        self.layer = 'bronze'
        self.log = getLogger('airflow.task')
        self.config = Config().load_config()
        self.url = self.config['job']['url']
        self.data_path = self.config["data_path"]

    def download(self, file_name):
        """
        Download site file.

        Parameters
        ----------
        file_name : str
            File name.

        Returns
        -------
        None
        """

        output_file = f'{self.data_path}/{self.layer}/{file_name}'
        if os_path.isfile(output_file):
            self.log.info(f'{file_name} already exist.')
        else:
            path = f'{file_name.split("_")[0]}.zip'
            url = f'{self.url}/{path}'
            self.log.info(f'downloading {path}...')
            wget_download(url, out=output_file, bar=None)
            self.log.info(f'{path} downloaded.')

    def files(self):
        """
        Download site files.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('files...')

        files = self.file.load(f'{self.layer}/download_files')

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(self.download, files))

        self.log.info(f'files: {len(files)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'files done! {elapsed_time}s')

    def run(self):
        """
        Run extract.

        Parameters
        ----------
        None.

        Returns
        -------
        None    
        """

        start_time = time()

        self.log.info(f'extract...')

        try:
            self.start.run()
            self.files()
        except Exception:
            raise

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'extract done! {elapsed_time}s')
