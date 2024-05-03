# coding: utf-8
""" Debug Job Module """

# built-in
from time import tzset, time
from logging import getLogger
from sys import path as sys_path
from os import environ as os_environ, getcwd as os_getcwd

# add the folder dags in sys path
JOB_PATH = os_getcwd()
while JOB_PATH in sys_path:
    # remove path script
    sys_path.remove(JOB_PATH)
# remove name job of string
JOB_PATH = '/'.join(JOB_PATH.split('/')[:-1])
sys_path.append(JOB_PATH)
os_environ['AIRFLOW_HOME'] = JOB_PATH.replace('/dags', '')

# installed


# custom
from cnpj.config import Config
from cnpj.src.bll.main import Main


class Run:
    """
    Run job.

    Attributes
    ----------
    None.

    Methods
    -------
    run_job()
        Execute job.
    """

    def __init__(self):
        self.main = Main()
        self.log = getLogger('airflow.task')
        self.config = Config().load_config()

    def execute_job(self):
        """
        Execute job.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        os_environ['TZ'] = self.config['environment']['timezone']
        tzset()

        self.log.info('***** start *****')

        self.main.run_start(self.config)
        self.main.run_extract(self.config)
        self.main.run_transform(self.config)
        # self.main.run_load(self.config)
        # self.main.run_end()

        self.log.info('***** end *****')


if __name__ == '__main__':
    Run().execute_job()
