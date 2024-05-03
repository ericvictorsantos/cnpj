# coding: utf-8
""" End Job Module """

# built-in
from time import time
from datetime import datetime

# installed
from logging import getLogger
from dateutil.relativedelta import relativedelta

# custom
from cnpj.src.dal.database import CNPJBase


class End:
    """
    Tasks after the ETL process.

    Attributes
    ----------
    None.

    Methods
    -------
    None.
    """

    def __init__(self):
        self.cnpj_base = CNPJBase()
        self.log = getLogger('airflow.task')

    def close_database_connection(self):
        """
        Close database connection.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('close_database_connection...')

        self.cnpj_base.close_database_connection()

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'close_database_connection done! {elapsed_time}s')

    def delete_expired_data(self, tenant_id):
        """
        Delete expired data.

        Parameters
        ----------
        tenant_id : str
            Tenant id.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('delete_expired_data...')

        date_time = datetime.utcnow()
        expiration_date = datetime(date_time.year, date_time.month, 1)
        expiration_date = expiration_date - relativedelta(months=self.data_validity)
        self.log.info(f'expiration_date: {expiration_date.date()}')
        deleted = self.tenant_base.delete_expired_data(tenant_id, expiration_date)
        self.log.info(f'deleted: {deleted}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'delete_expired_data done! {elapsed_time}s')
