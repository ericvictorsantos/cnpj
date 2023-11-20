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
    None.

    Methods
    -------
    None.
    """

    def __init__(self):
        self.file = File()
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
        if len(files) > 0:
            files = [f'bronze/{file}' for file in files]
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
