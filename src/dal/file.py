# coding: utf-8
""" File """

# built-in
from glob import glob
from pathlib import Path
from logging import getLogger
from os import path as os_path, remove as os_remove
from pickle import dump as pck_save, load as pck_load

# installed
from pandas import DataFrame, read_parquet as pd_read_parquet

# custom
from cnpj.config import Config


class File:
    """
    Class data file.

    Attributes
    ----------
    None.

    Methods
    -------
    run_job()
        Execute job.
    """

    def __init__(self):
        self.log = getLogger('airflow.task')
        self.config = Config().load_config()
        self.data_path = self.config['data_path']
        self.local_or_cloud = self.config['environment']['local_or_cloud']

    def clear(self):
        """
        Clear temporary data.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        if self.local_or_cloud == 'cloud':
            for layer in ['bronze', 'silver', 'gold']:
                self.log.info(f'----- {layer} -----')
                file_paths = glob(f'{self.data_path}/{layer}/*.bin')
                if len(file_paths) > 0:
                    for file_path in file_paths:
                        if os_path.isfile(file_path):
                            os_remove(file_path)
                            self.log.info(f'{os_path.basename(file_path)} deleted.')
                else:
                    self.log.info('no temp files to delete.')
                self.log.info(f'----- {layer} -----')

    def delete(self, file_paths):
        """
        Delete files

        Parameters
        ----------
        file_paths : list[str]
            File path.

        Returns
        -------
        None
        """

        for file_path in file_paths:
            file_path = f'{self.data_path}/{file_path}'
            if os_path.exists(file_path):
                os_remove(file_path)

    def exists(self, file_path):
        """
        File exists.

        Parameters
        ----------
        file_path : str
            File path.

        Returns
        -------
        flag : bool
            If path exists.
        """

        file_path = f'{self.data_path}/{file_path}'

        flag = True if os_path.exists(file_path) else False

        return flag

    def files(self, file_name):
        """
        Find zip files.

        Parameters
        ----------
        file_name : str
            File name to search.

        Returns
        -------
        files : list[str]
            File zip names.
        """

        files = glob(f'{self.data_path}/{file_name}')

        return files

    def load(self, file_name):
        """
        Load data.

        Parameters
        ----------
        file_name : str
            File name to data.

        Returns
        -------
        data : Any
            Any type with values.
        """

        file_path = f'{self.data_path}/{file_name}'

        if os_path.exists(file_path):
            if Path(file_path).suffix == '.parquet':
                data = pd_read_parquet(file_path)
            else:
                with open(file_path, 'rb') as context:
                    data = pck_load(context)
        else:
            data = None

        return data

    def save(self, data, file_name):
        """
        Write data.

        Parameters
        ----------
        data : Any
            Data.
        file_name : str
            File name to file data.

        Returns
        -------
        None
        """

        file_path = f'{self.data_path}/{file_name}'
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        if Path(file_path).suffix == '.parquet':
            data.to_parquet(f'{file_path}')
        else:
            with open(file_path, 'wb') as context:
                pck_save(data, context)
