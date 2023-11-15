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

    def compare_files(self):
        """
        Compare local and site files.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('compare_files...')

        site_files = self.file.load(f'bronze/site_files.bin')
        local_files = self.file.load(f'bronze/local_files.bin')

        delete_files = sorted(list(set(local_files) - set(site_files)))
        self.log.info(f'delete files: {len(delete_files)}')
        self.file.save(delete_files, 'bronze/delete_files.bin')

        download_files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'download files: {len(download_files)}')
        self.file.save(download_files, 'bronze/download_files.bin')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'compare_files done! {elapsed_time}s')

    def create_layers(self):
        """
        Create structure folder.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('create_folder_structure...')

        self.file.layers()

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'create_folder_structure done! {elapsed_time}s')

    def delete_files(self):
        """
        Delete local files.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('delete_files...')

        files = self.file.load(f'bronze/delete_files.bin')
        for file in files:
            file_path = f'{self.data_path}/bronze/{file}'
            if os_path.isfile(file_path):
                os_remove(file_path)
                self.log.info(f'deleted: {file}')
        else:
            self.log.info(f'delete files: {len(files)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'delete_files done! {elapsed_time}s')

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
        for file in files:
            os_remove(file)
            self.log.info(f'{os_path.basename(file)} deleted.')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'delete_temp_files done! {elapsed_time}s')

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
        file_name = f'bronze/local_files.bin'
        files = self.file.load(file_name)

        if files is None:
            files = self.file.files(f'bronze/*.zip')
            if len(files) > 0:
                files = [os_path.basename(file) for file in files]

            self.file.save(files, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'files: {len(files)}')

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
        file_name = f'bronze/site_files.bin'
        files = self.file.load(file_name)

        if files is None:
            self.log.info(f'url: {self.url}')
            page_text = req_get(self.url).text
            files = pd_read_html(page_text)[0]
            files.dropna(subset='Size', inplace=True)
            files = files[files['Name'].str.match(self.re_zip)]
            files.rename(columns={'Name': 'name', 'Last modified': 'last_modified'}, inplace=True)
            files['last_modified'] = files['last_modified'].str.replace(self.re_replace, '', regex=True).str[:8]
            files['file_name'] = files['name'].str.replace('.zip', '')
            files['file_name'] = files['file_name'] + '_' + files['last_modified'] + '.zip'
            files['last_modified'] = files['last_modified'].map(lambda date: datetime.strptime(date, '%Y%m%d'))
            files = files[['file_name', 'last_modified']]
            files = files['file_name'].tolist()

            self.file.save(files, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'files: {len(files)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'site_files done! {elapsed_time}s')

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
            self.create_layers()
            self.clear_temporary_data()
            self.delete_temp_files()
            self.site_files()
            self.local_files()
            self.compare_files()
            self.delete_files()
        except Exception:
            raise

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'start done! {elapsed_time}s')
