# coding: utf-8
""" Data Transformation Module """

# built-in
from time import time
from pathlib import Path
from zipfile import ZipFile
from logging import getLogger
from re import compile as re_compile
from os import path as os_path, cpu_count as os_cpu_count

# installed
from numpy import nan as np_nan
from pandas import read_csv as pd_read_csv, DataFrame as pd_DataFrame
from pyspark.sql import SparkSession, functions as f, types as sql_types

# custom
from cnpj.config import Config
from cnpj.src.dal.file import File


class Transform:
    """
    Data transformation Class

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
        self.log = getLogger('airflow.task')
        self.layer = 'silver'
        self.cores = os_cpu_count() // 2
        self.config = Config().load_config()
        self.data_path = self.config['data_path']
        self.chunk_size = self.config['job']['chunk_size']

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

        columns = [
            'cnpj_basico',
            'razao_social',
            'cod_natureza_juridica',
            'cod_qualificacao_responsavel',
            'capital_social',
            'cod_porte_empresa',
            'ente_federativo'
        ]

        site_files = self.file.load('bronze/site_files.bin')
        site_files = site_files['nome_arquivo'].tolist()
        site_files = [site_file.split('.')[0] for site_file in site_files if site_file.startswith('Emp')]

        self.log.info('----- Bronze -> Silver -----')
        local_files = sorted(self.file.files(f'silver/Emp*'))

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'{self.layer}/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'companies files: {len(files)}')

        # Pandas
        for file in files:
            self.log.info(f'{file.split("_")[0]}...')
            if not self.file.exists(f'{self.layer}/{file}'):
                with ZipFile(f'{self.data_path}/bronze/{file}.zip') as obj_zip:
                    csv_name = obj_zip.filelist[0].filename
                    csv = obj_zip.open(csv_name)
                    chunks = pd_read_csv(csv, sep=';', encoding='latin-1', header=None, dtype=str, names=columns, chunksize=self.chunk_size)
                    for idx, chunk in enumerate(chunks, start=1):
                        parquet_name = f'part_{str(idx).rjust(2, "0")}'
                        self.log.info(f'{parquet_name}...')
                        self.file.save(chunk, f'{self.layer}/{file}/{parquet_name}.parquet')
                        self.log.info(f'{parquet_name} done!')
            else:
                self.log.info(f'{file} already exists.')
            self.log.info(f'{file.split("_")[0]} done!')
        self.log.info('----- Bronze -> Silver -----')

        self.log.info('----- Silver -> Gold -----')
        local_files = sorted(self.file.files(f'gold/Emp*'))

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'{self.layer}/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'companies files: {len(files)}')

        # Spark
        spark = SparkSession.builder.master(f'local[{self.cores}]').getOrCreate()
        for file in files:
            self.log.info(f'{file.split("_")[0]}...')
            if not self.file.exists(f'{self.data_path}/gold/{file}'):
                file_parquets = sorted(self.file.files(f'{self.layer}/{file}/*.parquet'))
                for file_parquet in file_parquets:
                    parquet_name = file_parquet.split('.')[0]
                    parquet_path = f'{self.data_path}/{self.layer}/{file}/{parquet_name}.parquet'

                    self.log.info(f'{parquet_name}...')
                    companies = spark.read.parquet(parquet_path)

                    companies = companies.drop('ente_federativo')
                    companies = companies.dropDuplicates(subset=['cnpj_basico'])

                    companies = (
                        companies
                        .withColumn('capital_social',
                                    f.regexp_replace('capital_social', ',', '.'))
                    )

                    companies = (
                        companies
                        .withColumn('cnpj_basico',
                                    companies.cnpj_basico.cast(sql_types.IntegerType()))
                        .withColumn('cod_natureza_juridica',
                                    companies.cod_natureza_juridica.cast(sql_types.IntegerType()))
                        .withColumn('cod_qualificacao_responsavel',
                                    companies.cod_qualificacao_responsavel.cast(sql_types.IntegerType()))
                        .withColumn('capital_social',
                                    companies.capital_social.cast(sql_types.FloatType()))
                        .withColumn('cod_porte_empresa',
                                    companies.cod_porte_empresa.cast(sql_types.IntegerType()))
                    )

                    companies = (
                        companies
                        .withColumn('atualizado_em', f.current_timestamp())
                    )

                    companies = companies.replace(np_nan, None)
                    companies = companies.replace('NaN', None)
                    companies = companies.replace('NAN', None)

                    parquet_path = f'{self.data_path}/gold/{file}/{parquet_name}.parquet'
                    Path(parquet_path).parent.mkdir(parents=True, exist_ok=True)
                    companies.write.parquet(parquet_path)
                    self.log.info(f'{parquet_name} done!.')
            else:
                self.log.info(f'{file} already exists.')
            self.log.info(f'{file.split("_")[0]} done!')
        spark.stop()
        self.log.info('----- Silver -> Gold -----')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'companies done! {elapsed_time}s')

    def domains(self):
        """
        Table of domains.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('domains...')
        columns = ['codigo', 'descricao']
        re_domains = re_compile(r"(Cnae|Moti|Munic|Natu|Pais|Qual)")

        site_files = self.file.load('bronze/site_files.bin')
        site_files = site_files['nome_arquivo'].tolist()
        site_files = [file.split('.')[0] for file in site_files if re_domains.search(file)]

        local_files = sorted(self.file.files(f'silver/*.parquet'))
        local_files = [os_path.basename(file).split('.')[0] for file in local_files if re_domains.search(file)]

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'{self.layer}/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'companies files: {len(files)}')

        for file in files:
            self.log.info(f'{file.split("_")[0]}...')
            output_file_name = f'{file}.parquet'
            if not self.file.exists(f'{self.layer}/{output_file_name}'):
                with ZipFile(f'{self.data_path}/bronze/{file}.zip') as obj_zip:
                    input_file_name = obj_zip.filelist[0].filename
                    input_file = obj_zip.open(input_file_name)
                    output_file = pd_read_csv(input_file, sep=';', encoding='latin-1', header=None, names=columns)
                    output_file['descricao'] = output_file['descricao'].str.upper()
                    self.file.save(output_file, f'{self.layer}/{output_file_name}')
                    self.log.info(f'{output_file_name} done!.')
            else:
                self.log.info(f'{output_file_name} already exists.')
            self.log.info(f'{file.split("_")[0]} done!')

        # porte empresa
        file_name = 'Porte_' + site_files[0].rsplit('_')[-1]
        self.log.info(f'{file_name.split("_")[0]}...')
        file_path = f'{self.layer}/{file_name}.parquet'
        if not self.file.exists(file_path):
            data = {
                'codigo': [0, 1, 3, 5],
                'descricao': ['NÃO INFORMADO', 'MICRO EMPRESA', 'EMPRESA DE PEQUENO PORTE', 'DEMAIS']
            }
            data = pd_DataFrame(data)
            self.file.save(data, file_path)
            self.log.info(f'{file_name.split("_")[0]} done!')
        else:
            self.log.info(f'{file_name} already exists.')

        # matriz filial
        file_name = 'Matriz_' + site_files[0].rsplit('_')[-1]
        self.log.info(f'{file_name.split("_")[0]}...')
        file_path = f'{self.layer}/{file_name}.parquet'
        if not self.file.exists(file_path):
            data = {
                'codigo': [1, 2],
                'descricao': ['MATRIZ', 'FILIAL']
            }
            data = pd_DataFrame(data)
            self.file.save(data, file_path)
            self.log.info(f'{file_name.split("_")[0]} done!')
        else:
            self.log.info(f'{file_name} already exists.')

        # situação cadastral
        file_name = 'Situacao_' + site_files[0].rsplit('_')[-1]
        self.log.info(f'{file_name.split("_")[0]}...')
        file_path = f'{self.layer}/{file_name}.parquet'
        if not self.file.exists(file_path):
            data = {
                'codigo': [1, 2, 3, 4, 8],
                'descricao': ['NULA', 'ATIVA', 'SUSPENSA', 'INAPTA', 'BAIXADA']
            }
            data = pd_DataFrame(data)
            self.file.save(data, file_path)
            self.log.info(f'{file_name.split("_")[0]} done!')
        else:
            self.log.info(f'{file_name} already exists.')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'domains done! {elapsed_time}s')

    def institutions(self):
        """
        Transform institutions.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('institutions...')

        columns = [
            'cnpj_basico',
            'cnpj_order',
            'cnpj_dv',
            'cod_matriz_filial',
            'nome_fantasia',
            'cod_situacao_cadastral',
            'data_situacao_cadastral',
            'cod_motivo_situacao_cadastral',
            'nome_cidade_exterior',
            'cod_pais',
            'data_inicio_atividade',
            'cod_cnae_principal',
            'cod_cnae_secundaria',
            'tipo_logradouro',
            'logradouro',
            'numero',
            'complemento',
            'bairro',
            'cep',
            'uf',
            'cod_municipio',
            'ddd_1',
            'telefone_1',
            'ddd_2',
            'telefone_2',
            'ddd_fax',
            'fax',
            'correio_eletronico',
            'situacao_especial',
            'data_situacao_especial'
        ]

        site_files = self.file.load('bronze/site_files.bin')
        site_files = site_files['nome_arquivo'].tolist()
        site_files = [site_file.split('.')[0] for site_file in site_files if site_file.startswith('Est')]

        self.log.info('----- Bronze -> Silver -----')
        local_files = sorted(self.file.files(f'silver/Est*'))

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'{self.layer}/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'institutions files: {len(files)}')

        # Pandas
        for file in files:
            self.log.info(f'{file.split("_")[0]}...')
            if not self.file.exists(f'{self.layer}/{file}'):
                with ZipFile(f'{self.data_path}/bronze/{file}.zip') as obj_zip:
                    csv_name = obj_zip.filelist[0].filename
                    csv = obj_zip.open(csv_name)
                    chunks = pd_read_csv(csv, sep=';', encoding='latin-1', header=None, dtype=str, names=columns, chunksize=self.chunk_size)
                    for idx, chunk in enumerate(chunks, start=1):
                        parquet_name = f'part_{str(idx).rjust(2, "0")}'
                        self.log.info(f'{parquet_name}...')
                        self.file.save(chunk, f'{self.layer}/{file}/{parquet_name}.parquet')
                        self.log.info(f'{parquet_name} done!.')
            else:
                self.log.info(f'{file} already exists.')
            self.log.info(f'{file.split("_")[0]} done!')
        self.log.info('----- Bronze -> Silver -----')

        self.log.info('----- Silver -> Gold -----')
        local_files = sorted(self.file.files(f'gold/Est*'))

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'{self.layer}/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'institutions files: {len(files)}')

        # Spark
        spark = SparkSession.builder.master(f'local[{self.cores}]').getOrCreate()
        for file in files:
            self.log.info(f'{file.split("_")[0]}...')
            if not self.file.exists(f'{self.data_path}/gold/{file}'):
                file_parquets = sorted(self.file.files(f'{self.layer}/{file}/*.parquet'))
                for file_parquet in file_parquets:
                    parquet_name = file_parquet.split('.')[0]
                    parquet_path = f'{self.data_path}/{self.layer}/{file}/{parquet_name}.parquet'

                    self.log.info(f'{parquet_name}...')
                    institutions = spark.read.parquet(parquet_path)

                    institutions = (
                        institutions
                        .withColumn('cnpj_completo', f.concat('cnpj_basico', 'cnpj_order', 'cnpj_dv'))
                    )

                    institutions = institutions.drop('cnpj_order', 'cnpj_dv')
                    institutions = institutions.dropDuplicates(subset=['cnpj_completo'])

                    institutions = (
                        institutions
                        .withColumn('cnpj_basico',
                                    institutions.cnpj_basico.cast(sql_types.IntegerType()))
                        .withColumn('cod_matriz_filial',
                                    institutions.cod_matriz_filial.cast(sql_types.IntegerType()))
                        .withColumn('cod_situacao_cadastral',
                                    institutions.cod_situacao_cadastral.cast(sql_types.IntegerType()))
                        .withColumn('cod_motivo_situacao_cadastral',
                                    institutions.cod_motivo_situacao_cadastral.cast(sql_types.IntegerType()))
                        .withColumn('cod_pais',
                                    institutions.cod_pais.cast(sql_types.IntegerType()))
                        .withColumn('cod_cnae_principal',
                                    institutions.cod_cnae_principal.cast(sql_types.IntegerType()))
                        .withColumn('cod_municipio',
                                    institutions.cod_municipio.cast(sql_types.IntegerType()))
                    )

                    institutions = (
                        institutions
                        .withColumn('data_situacao_cadastral',
                                    f.when(f.col('data_situacao_cadastral').rlike(r'^(19|20)\d{6}'),
                                           f.col('data_situacao_cadastral')).otherwise(None))
                        .withColumn('data_inicio_atividade',
                                    f.when(f.col('data_inicio_atividade').rlike(r'^(19|20)\d{6}'),
                                           f.col('data_inicio_atividade')).otherwise(None))
                        .withColumn('data_situacao_especial',
                                    f.when(f.col('data_situacao_especial').rlike(r'^(19|20)\d{6}'),
                                           f.col('data_situacao_especial')).otherwise(None))
                    )

                    institutions = (
                        institutions
                        .withColumn('data_situacao_cadastral',
                                    f.to_date('data_situacao_cadastral', 'yyyyMMdd'))
                        .withColumn('data_inicio_atividade',
                                    f.to_date('data_inicio_atividade', 'yyyyMMdd'))
                        .withColumn('data_situacao_especial',
                                    f.to_date('data_situacao_especial', 'yyyyMMdd'))
                    )

                    institutions = (
                        institutions
                        .withColumn('logradouro', f.upper('logradouro'))
                        .withColumn('bairro', f.upper('bairro'))
                        .withColumn('correio_eletronico', f.upper('correio_eletronico'))
                    )

                    institutions = (
                        institutions
                        .withColumn('complemento',
                                    f.regexp_replace('complemento', r'\s{2,}', ' '))
                        .withColumn('logradouro',
                                    f.regexp_replace('logradouro', r'\s{2,}', ' '))
                        .withColumn('complemento',
                                    f.regexp_replace('complemento', r'\x00', ' '))
                        .withColumn('logradouro',
                                    f.regexp_replace('logradouro', r'\x00', ' '))
                    )

                    institutions = (
                        institutions
                        .withColumn('atualizado_em', f.current_timestamp())
                    )

                    institutions = institutions.replace(np_nan, None)
                    institutions = institutions.replace('NaN', None)
                    institutions = institutions.replace('NAN', None)

                    parquet_path = f'{self.data_path}/gold/{file}/{parquet_name}.parquet'
                    Path(parquet_path).parent.mkdir(parents=True, exist_ok=True)
                    institutions.write.parquet(parquet_path)
                    self.log.info(f'{parquet_name} done!.')
            else:
                self.log.info(f'{file} already exists.')
            self.log.info(f'{file.split("_")[0]} done!')
        spark.stop()
        self.log.info('----- Silver -> Gold -----')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'institutions done! {elapsed_time}s')

    def run(self):
        """
        Run transform.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        self.log.info('----- Transform -----')

        try:
            self.domains()
            self.companies()
            self.institutions()
        except Exception:
            raise

        self.log.info('----- Transform -----')
