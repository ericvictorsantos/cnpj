# coding: utf-8
""" File """

# built-in
from glob import glob
from pathlib import Path
from logging import getLogger
from os import path as os_path, remove as os_remove
from pickle import dump as pck_save, load as pck_load

# installed

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
        self.data_dir = f'{self.config["job"]["path"]}/data'
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
                file_paths = glob(f'{self.data_dir}/{layer}/*.bin')
                if len(file_paths) > 0:
                    for file_path in file_paths:
                        if os_path.isfile(file_path):
                            os_remove(file_path)
                            self.log.info(f'{os_path.basename(file_path)} deleted.')
                else:
                    self.log.info('no temp files to delete.')
                self.log.info(f'----- {layer} -----')

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

        files = glob(f'{self.data_dir}/{file_name}')

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
        data : any
            Any type with values.
        """

        file = f'{self.data_dir}/{file_name}.bin'

        if os_path.isfile(file):
            with open(file, 'rb') as context:
                data = pck_load(context)
        else:
            data = None

        return data

    def save(self, data, file_name):
        """
        Write data.

        Parameters
        ----------
        data : any
            Any type data.
        file_name : str
            File name to file data.

        Returns
        -------
        None
        """

        file = f'{self.data_dir}/{file_name}.bin'

        Path(file).parent.mkdir(parents=True, exist_ok=True)
        with open(file, 'wb') as context:
            pck_save(data, context)
