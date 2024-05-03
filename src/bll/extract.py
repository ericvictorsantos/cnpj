# coding: utf-8
""" Data Extraction Module """

# built-in
from time import time
from datetime import datetime
from re import compile as re_compile
from concurrent.futures import ThreadPoolExecutor

# installed
from logging import getLogger
from requests import get as req_get
from wget import download as wget_download
from pandas import read_html as pd_read_html

# custom
from cnpj.src.dal.file import File


class Extract:
    """
    Data extraction class.

    Attributes
    ----------

    Methods
    -------
    None.
    """

    def __init__(self, config):
        self.file = File(config)
        self.log = getLogger('airflow.task')
        self.layer = 'bronze'
        self.max_workers = 4
        self.url = config['params']['url']
        self.data_path = config['data']['path']

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
        site_files = site_files['nome_arquivo'].tolist()
        site_files = [file for file in site_files if file.startswith('Emp')]

        local_files = self.file.files(f'{self.layer}/Emp*')

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'{self.layer}/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        if len(files) > 0:
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                list(pool.map(self.download, files))
        self.log.info(f'download files: {len(files)}')

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
        site_files = site_files['nome_arquivo'].tolist()
        site_files = [file for file in site_files if re_domains.search(file)]

        local_files = self.file.files(f'{self.layer}/*.zip')
        local_files = [file for file in local_files if re_domains.search(file)]

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'{self.layer}/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        if len(files) > 0:
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                list(pool.map(self.download, files))
        self.log.info(f'download files: {len(files)}')

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
        site_files = site_files['nome_arquivo'].tolist()
        site_files = [file for file in site_files if file.startswith('Est')]

        local_files = self.file.files(f'{self.layer}/Est*')

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'{self.layer}/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        if len(files) > 0:
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                list(pool.map(self.download, files))
        self.log.info(f'download files: {len(files)}')

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

        if not self.file.exists(file_path):
            re_replace = re_compile(r'[\s:-]+')
            re_zip = re_compile(r"(Empre|Estabele|Cnae|Moti|Munic|Natu|Pais|Qual).*zip")

            self.log.info(f'url: {self.url}')
            page_text = req_get(self.url).text
            files = pd_read_html(page_text)[0]
            files.dropna(subset='Size', inplace=True)
            files = files[files['Name'].str.match(re_zip)]
            files.rename(columns={'Name': 'nome', 'Last modified': 'ultima_extracao'}, inplace=True)
            files['ultima_extracao'] = files['ultima_extracao'].str.replace(re_replace, '', regex=True).str[:8]
            files['nome_arquivo'] = files['nome'].str.replace('.zip', '')
            files['nome_arquivo'] = files['nome_arquivo'] + '_' + files['ultima_extracao'] + '.zip'
            files['ultima_extracao'] = files['ultima_extracao'].map(lambda date: datetime.strptime(date, '%Y%m%d'))
            files = files[['nome', 'ultima_extracao', 'nome_arquivo']]

            self.file.save(files, file_path)
        else:
            self.log.info(f'{file_name} already exists. ')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'site_files done! {elapsed_time}s')
