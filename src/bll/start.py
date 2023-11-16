# coding: utf-8
""" Start Job Module """

# built-in
from time import time
from datetime import datetime
from re import compile as re_compile
from os import path as os_path, remove as os_remove, mkdir as os_mkdir

# installed
from logging import getLogger
from requests import get as req_get
from pandas import read_html as pd_read_html

# custom
from cnpj.config import Config
from cnpj.src.dal.file import File


class Start:
    """
    Tasks before the ETL process.

    Attributes
    ----------
    None.

    Methods
    -------
    None.
    """

    def __init__(self):
        self.file = File()
        self.log = getLogger('airflow.task')
        self.config = Config().load_config()
        self.url = self.config['job']['url']
        self.re_replace = re_compile(r'[\s:-]+')
        self.data_path = self.config['data_path']
        self.re_zip = re_compile(r"(Empre|Estabele|Simples|Cnae|Moti|Munic|Natu|Pais|Qual).*zip")

    def clear_temporary_data(self):
        """
        Clear temporary data.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('clear_temporary_data...')

        self.file.clear()

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'clear_temporary_data done! {elapsed_time}s')

    def delete_temp_files(self):
        """
        Delete temporary (.tmp) files.

        Parameters
        ----------
        None.

        Returns
        -------
        files : list[str]
            Local file names.
        """

        start_time = time()
        self.log.info('delete_temp_files...')

        files = self.file.files(f'bronze/*.tmp')
        self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'delete_temp_files done! {elapsed_time}s')

    def run(self):
        """
        Run start.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()

        self.log.info(f'start...')

        try:
            self.clear_temporary_data()
            self.delete_temp_files()
        except Exception:
            raise

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'start done! {elapsed_time}s')
