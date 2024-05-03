# coding: utf-8
""" Data Load Module """

# built-in
from time import time
from datetime import datetime
from re import compile as re_compile
from os import cpu_count as os_cpu_count

# installed
from logging import getLogger
from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook
from pandas import read_parquet as pd_read_parquet

# custom
from cnpj.src.dal.file import File
from cnpj.src.dal.database import CNPJBase


class Load:
    """
    Data load class.

    Attributes
    ----------

    Methods
    -------
    run_job()
        Execute job.
    """

    def __init__(self, config):
        self.file = File(config)
        self.cnpj_base = CNPJBase()
        self.log = getLogger('airflow.task')
        self.conn = BaseHook.get_connection('cnpj')
        self.layer = 'gold'
        self.cores = os_cpu_count() // 2
        self.data_path = config['data']['path']

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

                self.file_info(file)
            else:
                self.log.info(f'{file.split("_")[0]} imported!')

            self.log.info(f'{file.split("_")[0]} done!')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'companies done! {elapsed_time}s')

    def domains(self):
        """
        Load domains.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('domains...')
        re_domains = re_compile(r"(Cnae|Moti|Munic|Natu|Pais|Qual|Porte|Matriz|Situacao)")
        table_name = {'Cnaes': 'cnae', 'Matriz': 'matriz_filial', 'Motivos': 'motivo_cadastral',
                      'Municipios': 'municipio', 'Naturezas': 'natureza_juridica', 'Paises': 'pais',
                      'Porte': 'porte_empresa', 'Qualificacoes': 'qualificacao_socio', 'Situacao': 'situacao_cadastral'}

        imported_files = self.cnpj_base.get_imported_files()
        imported_files = [file[0].split('.')[0] for file in imported_files if re_domains.search(file[0])]

        files = sorted(self.file.files(f'silver/*.parquet'))
        files = [file.split('.')[0] for file in files]
        for file in files:
            file_name = file.split("_")[0]
            self.log.info(f'{file_name}...')
            if file not in imported_files:
                file_path = f'{self.data_path}/silver/{file}.parquet'
                domains = pd_read_parquet(file_path)
                domains = domains.to_dict(orient='records')
                table = table_name[file_name]
                self.cnpj_base.set_domains(table, domains)
                self.file_info(file)
            else:
                self.log.info(f'{file.split("_")[0]} imported!')

            self.log.info(f'{file.split("_")[0]} done!')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'domains done! {elapsed_time}s')

    def file_info(self, file):
        """
        Load file info.

        Parameters
        ----------
        file : str
            File name.

        Returns
        -------
        None
        """

        file_info = {}

        name, last_extraction = file.split('_')
        last_extraction = datetime.strptime(last_extraction, '%Y%m%d')

        file_info['nome'] = name
        file_info['nome_arquivo'] = file
        file_info['ultima_extracao'] = last_extraction
        file_info['atualizado_em'] = datetime.now()
        self.cnpj_base.set_file_info([file_info])
        self.log.info(f'{name} set file info!')

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

                self.file_info(file)
            else:
                self.log.info(f'{file.split("_")[0]} imported!')

            self.log.info(f'{file.split("_")[0]} done!')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'institutions done! {elapsed_time}s')
