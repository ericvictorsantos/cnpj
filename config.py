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
    job_name : str
        Name of job.

    Methods
    -------
    load_config()
        Load configuration job from toml.
    """

    def __init__(self, key=None):
        self.job_path = os_path.dirname(__file__)
        self.job_name = os_path.basename(self.job_path)

    def load_config(self):
        """
        Load configuration from .toml.

        Parameters
        ----------
        None.

        Returns
        -------
        config : dict
            Job configuration.
        """

        config = {}
        default_params = toml_load(f'{self.job_path}/config.toml')

        config['job'] = {
            'path': self.job_path,
            'name': self.job_name
        }
        config['data'] = {
            'path': self.job_path.replace('dags', 'data')
        }
        config['environment'] = {
            'local_or_cloud': Variable.get('environment', default_var='local'),
            'timezone': Variable.get('timezone', default_var='America/Recife')
        }

        variable_job = Variable.get(key=self.job_name, default_var=None, deserialize_json=True)
        if variable_job:
            config['params'] = variable_job.get('params', default_params)
        else:
            config['params'] = default_params

        return config
