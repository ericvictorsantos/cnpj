# coding: utf-8
""" Config Application """

# built-in
from os import path as os_path

# installed
from toml import load as toml_load
from airflow.models import Variable

# custom


class Config:
    """
    Class configuration job.

    Attributes
    ----------
    job_path : str
        Path of job.
    key : str
        Key from get dict.

    Methods
    -------
    load_config()
        Load configuration job from toml.
    """

    def __init__(self, key=None):
        self.job_path = os_path.dirname(__file__)
        self.job_name = os_path.basename(self.job_path)
        self.local_or_cloud = Variable.get('environment', default_var='local')
        self.timezone = Variable.get('timezone', default_var='America/Recife')
        self.key = key

    def load_config(self):
        """
        Load configuration from .toml.

        Parameters
        ----------
        None.

        Returns
        -------
        config : dict
            Dict configuration.
        """

        config = toml_load(f'{self.job_path}/config.toml')
        config['job']['path'] = self.job_path
        config['job']['name'] = self.job_name
        config['data_path'] = self.job_path.replace('dags', 'data')
        config['environment'] = {'local_or_cloud': self.local_or_cloud}
        config['environment'].update({'timezone': self.timezone})

        if self.key:
            config = config[self.key]

        return config
