# coding: utf-8
""" Main Module """

# built-in
from logging import getLogger

# installed

# custom
from cnpj.src.bll.end import End
from cnpj.src.bll.load import Load
from cnpj.src.bll.start import Start
from cnpj.src.bll.extract import Extract
from cnpj.src.bll.transform import Transform


class Main:
    """
    Class main of job.

    Attributes
    ----------
    None.

    Methods
    -------
    run_job()
        Execute job.
    """

    def __init__(self):
        self.end = None
        self.load = None
        self.start = None
        self.extract = None
        self.transform = None
        self.log = getLogger('airflow.task')

    def run_transform(self, config):
        """
        Run data transform.

        Parameters
        ----------
        config : dict
            Job configuration.

        Returns
        -------
        None
        """

        self.log.info('----- Transform -----')

        try:
            self.transform = Transform(config)
            self.transform.domains()
            self.transform.companies()
            self.transform.institutions()
        except Exception:
            raise

        self.log.info('----- Transform -----')

    def run_load(self, config):
        """
        Run data load.

        Parameters
        ----------
        config : dict
            Job configuration.

        Returns
        -------
        None
        """

        self.log.info('----- Load -----')

        try:
            self.load = Load(config)
            self.load.domains()
            self.load.companies()
            self.load.institutions()
        except Exception:
            raise

        self.log.info('----- Load -----')

    def run_start(self, config):
        """
        Run tasks before the ETL process.

        Parameters
        ----------
        config : dict
            Job configuration.

        Returns
        -------
        None
        """

        self.log.info('----- Start -----')

        try:
            self.start = Start(config)
            self.start.clear_temporary_data()
            self.start.create_layers()
        except Exception:
            raise

        self.log.info('----- Start -----')

    def run_end(self, config):
        """
        Run tasks after the ETL process.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        self.log.info('----- End -----')

        try:
            self.end = End()
            self.end.delete_expired_data()
        except Exception:
            raise

        self.log.info('----- Start -----')

    def run_extract(self, config):
        """
        Run data extract.

        Parameters
        ----------
        config : dict
            Job configuration.

        Returns
        -------
        None
        """

        self.log.info('----- Extract -----')

        try:
            self.extract = Extract(config)
            self.extract.site_files()
            self.extract.domains()
            self.extract.companies()
            self.extract.institutions()
        except Exception:
            raise

        self.log.info('----- Extract -----')
