# coding: utf-8
""" Start Job Module """

# built-in
from time import time

# installed
from logging import getLogger

# custom
from cnpj.src.dal.file import File


class Start:
    """
    Tasks before the ETL process.

    Attributes
    ----------

    Methods
    -------
    None.
    """

    def __init__(self, config):
        self.file = File(config)
        self.log = getLogger('airflow.task')

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

    def create_layers(self):
        """
        Create data layers.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('create_layers...')

        self.file.create_layers()

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'create_layers done! {elapsed_time}s')
