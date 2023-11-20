# coding: utf-8
""" Data Load Module """

# built-in
from time import time
from datetime import datetime
from os import cpu_count as os_cpu_count

# installed
from logging import getLogger
from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook

# custom
from cnpj.config import Config
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
        self.cnpj_base = CNPJBase()
        self.log = getLogger('airflow.task')
        self.layer = 'gold'
        self.cores = os_cpu_count() // 2
        self.config = Config().load_config()
        self.data_path = self.config['data_path']
        self.conn = BaseHook.get_connection('cnpj')

    def companies(self):
        """
        Transform companies.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('companies...')

        imported_files = self.cnpj_base.get_imported_files()
        imported_files = [file[0].split('.')[0] for file in imported_files if file[0].startswith('Emp')]

        files = sorted(self.file.files(f'{self.layer}/Emp*'))
        spark = SparkSession.builder.master(f'local[{self.cores}]').getOrCreate()
        for file in files:
            self.log.info(f'{file.split("_")[0]}...')
            if file not in imported_files:
                file_parquets = sorted(self.file.files(f'{self.layer}/{file}/*.parquet'))
                for file_parquet in file_parquets:
                    parquet_name = file_parquet.split('.')[0]
                    parquet_path = f'{self.data_path}/{self.layer}/{file}/{parquet_name}.parquet'
                    self.log.info(f'{parquet_name}...')
                    companies = spark.read.parquet(parquet_path)
                    start = time()
                    (
                        companies
                        .write
                        .format('jdbc')
                        .option('url', f'jdbc:postgresql://{self.conn.host}:{self.conn.port}/{self.conn.schema}')
                        .option("driver", "org.postgresql.Driver")
                        .option("dbtable", "empresa_temp")
                        .option("user", self.conn.login)
                        .option("password", self.conn.password)
                        .save(mode='overwrite')
                    )
                    print(f'insert: {time() - start}')
                    start = time()
                    self.cnpj_base.upsert_companies()
                    print(f'update: {time() - start}')
                    self.log.info(f'{parquet_name} done!.')

                site_files = self.file.load(f'bronze/site_files.bin')
                site_files = site_files[site_files['nome_arquivo'] == f'{file}.zip']
                site_files['atualizado_em'] = datetime.now()
                file_info = site_files.to_dict(orient='records')
                self.cnpj_base.set_file_info(file_info)
            else:
                self.log.info(f'{file.split("_")[0]} imported!')

            self.log.info(f'{file.split("_")[0]} done!')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'companies done! {elapsed_time}s')

    def institutions(self):
        """
        Load institutions.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('institutions...')

        imported_files = self.cnpj_base.get_imported_files()
        imported_files = [file[0].split('.')[0] for file in imported_files if file[0].startswith('Est')]

        files = sorted(self.file.files(f'{self.layer}/Est*'))
        spark = SparkSession.builder.master(f'local[{self.cores}]').getOrCreate()
        for file in files:
            self.log.info(f'{file.split("_")[0]}...')
            if file not in imported_files:
                file_parquets = sorted(self.file.files(f'{self.layer}/{file}/*.parquet'))
                for file_parquet in file_parquets:
                    parquet_name = file_parquet.split('.')[0]
                    parquet_path = f'{self.data_path}/{self.layer}/{file}/{parquet_name}.parquet'
                    self.log.info(f'{parquet_name}...')
                    companies = spark.read.parquet(parquet_path)
                    start = time()
                    (
                        companies
                        .write
                        .format('jdbc')
                        .option('url', f'jdbc:postgresql://{self.conn.host}:{self.conn.port}/{self.conn.schema}')
                        .option("driver", "org.postgresql.Driver")
                        .option("dbtable", "estabelecimento_temp")
                        .option("user", self.conn.login)
                        .option("password", self.conn.password)
                        .save(mode='overwrite')
                    )
                    print(f'insert: {time() - start}')
                    start = time()
                    self.cnpj_base.upsert_institutions()
                    print(f'updated: {time() - start}')
                    self.log.info(f'{parquet_name} done!.')

                site_files = self.file.load(f'bronze/site_files.bin')
                site_files = site_files[site_files['nome_arquivo'] == f'{file}.zip']
                site_files['atualizado_em'] = datetime.now()
                file_info = site_files.to_dict(orient='records')
                self.cnpj_base.set_file_info(file_info)
            else:
                self.log.info(f'{file.split("_")[0]} imported!')

            self.log.info(f'{file.split("_")[0]} done!')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'institutions done! {elapsed_time}s')

    def run(self):
        """
        Run load.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        self.log.info('----- Load -----')

        try:
            # self.domains()
            self.companies()
            self.institutions()
        except Exception:
            raise

        self.log.info('----- Load -----')
