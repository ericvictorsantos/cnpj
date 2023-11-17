# coding: utf-8
""" Data Extraction Module """

# built-in
from time import time
from datetime import datetime
from os import path as os_path
from re import compile as re_compile
from concurrent.futures import ThreadPoolExecutor

# installed
from logging import getLogger
from requests import get as req_get
from wget import download as wget_download
from pandas import read_html as pd_read_html

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
        self.max_workers = 4

    def companies(self):
        """
        Transform companies.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('companies...')

        site_files = self.file.load(f'{self.layer}/site_files.bin')
        site_files = [file for file in site_files if file.startswith('Emp')]

        local_files = self.file.load(f'{self.layer}/local_files.bin')
        local_files = [file for file in local_files if file.startswith('Emp')]

        delete_files = sorted(list(set(local_files) - set(site_files)))
        delete_files = [f'{self.layer}/{file}' for file in delete_files]
        self.file.delete(delete_files)
        self.log.info(f'delete files: {len(delete_files)}')

        download_files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'download files: {len(download_files)}')

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            list(pool.map(self.download, download_files))

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'companies done! {elapsed_time}s')

    def domains(self):
        """
        Extract domains.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('domains...')
        re_domains = re_compile(r"(Cnae|Moti|Munic|Natu|Pais|Qual).*zip")

        site_files = self.file.load(f'{self.layer}/site_files.bin')
        site_files = [file for file in site_files if re_domains.search(file)]

        local_files = self.file.load(f'{self.layer}/local_files.bin')
        local_files = [file for file in local_files if re_domains.search(file)]

        delete_files = sorted(list(set(local_files) - set(site_files)))
        delete_files = [f'{self.layer}/{file}' for file in delete_files]
        self.file.delete(delete_files)
        self.log.info(f'delete files: {len(delete_files)}')

        download_files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'download files: {len(download_files)}')

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            list(pool.map(self.download, download_files))

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'domains done! {elapsed_time}s')

    def institutions(self):
        """
        Extract institutions.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('institutions...')

        site_files = self.file.load(f'{self.layer}/site_files.bin')
        site_files = [file for file in site_files if file.startswith('Est')]

        local_files = self.file.load(f'{self.layer}/local_files.bin')
        local_files = [file for file in local_files if file.startswith('Est')]

        delete_files = sorted(list(set(local_files) - set(site_files)))
        delete_files = [f'{self.layer}/{file}' for file in delete_files]
        self.file.delete(delete_files)
        self.log.info(f'delete files: {len(delete_files)}')

        download_files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'download files: {len(download_files)}')

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            list(pool.map(self.download, download_files))

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'institutions done! {elapsed_time}s')

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

        output_file = f'{self.layer}/{file_name}'
        if self.file.exists(output_file):
            self.log.info(f'{file_name} already exist.')
        else:
            path = f'{file_name.split("_")[0]}.zip'
            url = f'{self.url}/{path}'
            self.log.info(f'downloading {path}...')
            wget_download(url, out=f'{self.data_path}/{output_file}', bar=None)
            self.log.info(f'{path} downloaded.')

    def local_files(self):
        """
        Get local files.

        Parameters
        ----------
        None.

        Returns
        -------
        files : list[str]
            Local file names.
        """

        start_time = time()
        self.log.info('local_files...')
        file_name = 'local_files'
        file_path = f'{self.layer}/{file_name}.bin'

        if not self.file.exists(file_path):
            files = self.file.files(f'{self.layer}/*.zip')
            if len(files) > 0:
                files = [os_path.basename(file) for file in files]

            self.file.save(files, file_path)
        else:
            self.log.info(f'{file_name } already exists.')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'local_files done! {elapsed_time}s')

    def site_files(self):
        """
        Get site files.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('site_files...')
        file_name = 'site_files'
        file_path = f'{self.layer}/{file_name}.bin'
        re_replace = re_compile(r'[\s:-]+')
        re_zip = re_compile(r"(Empre|Estabele|Simples|Cnae|Moti|Munic|Natu|Pais|Qual).*zip")

        if not self.file.exists(file_path):
            self.log.info(f'url: {self.url}')
            page_text = req_get(self.url).text
            files = pd_read_html(page_text)[0]
            files.dropna(subset='Size', inplace=True)
            files = files[files['Name'].str.match(re_zip)]
            files.rename(columns={'Name': 'name', 'Last modified': 'last_modified'}, inplace=True)
            files['last_modified'] = files['last_modified'].str.replace(re_replace, '', regex=True).str[:8]
            files['file_name'] = files['name'].str.replace('.zip', '')
            files['file_name'] = files['file_name'] + '_' + files['last_modified'] + '.zip'
            files['last_modified'] = files['last_modified'].map(lambda date: datetime.strptime(date, '%Y%m%d'))
            files = files['file_name'].tolist()

            self.file.save(files, file_path)
        else:
            self.log.info(f'{file_name} already exists. ')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'site_files done! {elapsed_time}s')

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

        self.log.info('----- Extract -----')

        try:
            self.start.run()
            self.site_files()
            self.local_files()
            self.companies()
            self.institutions()
            self.domains()
        except Exception:
            raise

        self.log.info('----- Extract -----')
