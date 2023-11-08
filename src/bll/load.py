# coding: utf-8
""" Data Load Module """

# built-in
from time import time

# installed
from logging import getLogger

# custom
from cnpj.src.dal.file import File
from cnpj.src.dal.database import CNPJBase


class Load:
    """
    Data load class.

    Attributes
    ----------
    None.

    Methods
    -------
    run_job()
        Execute job.
    """

    def __init__(self):
        self.file = File()
        self.tenant_base = TenantBase()
        self.log = getLogger('airflow.task')
        self.layer = 'gold'

    def potentials(self, dag_id, tenant_id):
        """
        Get products and save local data.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_id : str
            Tenant id.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('load_potentials...')
        dag_path = f'{tenant_id}/{dag_id}'
        file_name = f'{dag_path}/{self.layer}/potentials'
        potentials = self.file.load(file_name)

        if len(potentials) > 0:
            potentials = potentials.to_dict(orient='records')
            self.tenant_base.set_potentials(tenant_id, potentials)

        self.log.info(f'potentials: {len(potentials)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'load_potentials done! {elapsed_time}s')
