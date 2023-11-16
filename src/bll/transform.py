# coding: utf-8
""" Data Transformation Module """

# built-in
from time import time
from zipfile import ZipFile
from logging import getLogger
from datetime import datetime
from os import path as os_path
from re import compile as re_compile

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
        self.config = Config().load_config()
        self.chunk_size = self.config['job']['chunk_size']
        self.data_path = self.config['data_path']
        self.date_time = datetime.utcnow()
        self.layer = 'silver'

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

        file_zips = sorted(self.file.files(f'bronze/Emp*.zip'))

        for file_zip in file_zips:
            zip_name = os_path.basename(file_zip).split('.')[0]
            self.log.info(f'{zip_name.split("_")[0]}...')
            with ZipFile(file_zip) as obj_zip:
                input_file_name = obj_zip.filelist[0].filename
                input_file = obj_zip.open(input_file_name)
                chunks = pd_read_csv(input_file, sep=';', encoding='latin-1', header=None, dtype=str, names=columns, chunksize=self.chunk_size)
                for idx, chunk in enumerate(chunks, start=1):
                    output_file_name = f'{zip_name}_{str(idx).rjust(2, "0")}.parquet'
                    if not self.file.exists(f'{self.layer}/{output_file_name}'):
                        self.file.save(chunk, f'{self.layer}/{output_file_name}')
                        self.log.info(f'{output_file_name} done!.')
                    else:
                        self.log.info(f'{output_file_name} already exists.')
            self.log.info(f'{zip_name.split("_")[0]} done!')

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
        re_domains = re_compile(r"(Cnae|Moti|Munic|Natu|Pais|Qual).*zip")

        file_zips = sorted(self.file.files(f'bronze/*.zip'))
        file_zips = [file_zip for file_zip in file_zips if re_domains.search(file_zip)]

        for file_zip in file_zips:
            zip_name = os_path.basename(file_zip).split('.')[0]
            self.log.info(f'{zip_name.split("_")[0]}...')
            output_file_name = f'{zip_name}.parquet'
            if not self.file.exists(f'{self.layer}/{output_file_name}'):
                with ZipFile(file_zip) as obj_zip:
                    input_file_name = obj_zip.filelist[0].filename
                    input_file = obj_zip.open(input_file_name)
                    output_file = pd_read_csv(input_file, sep=';', encoding='latin-1', header=None, names=columns)
                    output_file['descricao'] = output_file['descricao'].str.upper()
                    self.file.save(output_file, f'{self.layer}/{output_file_name}')
                    self.log.info(f'{output_file_name} done!.')
            else:
                self.log.info(f'{output_file_name} already exists.')
            self.log.info(f'{zip_name.split("_")[0]} done!')

        # porte empresa
        file_name = 'Porte_' + file_zips[0].rsplit('_')[-1].replace('zip', 'parquet')
        file_path = f'{self.layer}/{file_name}'
        if not self.file.exists(file_path):
            data = {
                'codigo': [0, 1, 3, 5],
                'descricao': ['NÃO INFORMADO', 'MICRO EMPRESA', 'EMPRESA DE PEQUENO PORTE', 'DEMAIS']
            }
            data = pd_DataFrame(data)
            self.file.save(data, file_path)
        else:
            self.log.info(f'{file_name} already exists.')

        # matriz filial
        file_name = 'Matriz_' + file_zips[0].rsplit('_')[-1].replace('zip', 'parquet')
        file_path = f'{self.layer}/{file_name}'
        if not self.file.exists(file_path):
            data = {
                'codigo': [1, 2],
                'descricao': ['MATRIZ', 'FILIAL']
            }
            data = pd_DataFrame(data)
            self.file.save(data, file_path)
        else:
            self.log.info(f'{file_name} already exists.')

        # situação cadastral
        file_name = 'Situacao_' + file_zips[0].rsplit('_')[-1].replace('zip', 'parquet')
        file_path = f'{self.layer}/{file_name}'
        if not self.file.exists(file_path):
            data = {
                'codigo': [1, 2, 3, 4, 8],
                'descricao': ['NULA', 'ATIVA', 'SUSPENSA', 'INAPTA', 'BAIXADA']
            }
            data = pd_DataFrame(data)
            self.file.save(data, file_path)
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

        # Pandas
        self.log.info('----- Bronze -> Silver -----')
        file_zips = sorted(self.file.files(f'bronze/Est*.zip'))
        # TODO
        file_zips = file_zips[1:2]
        for file_zip in file_zips:
            zip_name = os_path.basename(file_zip).split('.')[0]
            zip_path = f'{self.layer}/{zip_name}'
            self.log.info(f'{zip_name.split("_")[0]}...')
            if not self.file.exists(zip_path):
                with ZipFile(file_zip) as obj_zip:
                    input_file_name = obj_zip.filelist[0].filename
                    input_file = obj_zip.open(input_file_name)
                    chunks = pd_read_csv(input_file, sep=';', encoding='latin-1', header=None, dtype=str, names=columns, chunksize=self.chunk_size)
                    for idx, chunk in enumerate(chunks, start=1):
                        output_file_name = f'part_{str(idx).rjust(2, "0")}.parquet'
                        self.file.save(chunk, f'{zip_path}/{output_file_name}')
                        self.log.info(f'{output_file_name} done!.')
            else:
                self.log.info(f'{zip_name} already exists.')
            self.log.info(f'{zip_name.split("_")[0]} done!')
        self.log.info('----- Bronze -> Silver -----')

        # Spark
        self.log.info('----- Silver -> Gold -----')
        spark = SparkSession.builder.master('local[1]').getOrCreate()
        parquet_folders = sorted(self.file.files(f'{self.layer}/Est*'))
        for parquet_folder in parquet_folders:
            folder_name = parquet_folder.split('/')[-1]
            folder_path = f'gold/{folder_name}'
            self.log.info(f'{folder_name.split("_")[0]}...')
            if not self.file.exists(folder_path):
                file_parquets = sorted(self.file.files(f'silver/{folder_name}/*.parquet'))
                for file_parquet in file_parquets:
                    parquet_name = os_path.basename(file_parquet).split('.')[0]
                    parquet_path = f'{folder_path}/{parquet_name}.parquet'

                    institutions = spark.read.parquet(file_parquet)

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

                    # # join matriz filial
                    # data = spark.read.parquet(f'{self.data_path}/{self.layer}/Matriz_20231018.parquet')
                    # data = (
                    #     data
                    #     .withColumnRenamed('codigo', 'cod_matriz_filial')
                    #     .withColumnRenamed('descricao', 'desc_matriz_filial')
                    # )
                    # institutions = institutions.join(data, on='cod_matriz_filial')
                    #
                    # # join situacao cadastral
                    # data = spark.read.parquet(f'{self.data_path}/{self.layer}/Situacao_20231018.parquet')
                    # data = (
                    #     data
                    #     .withColumnRenamed('codigo', 'cod_situacao_cadastral')
                    #     .withColumnRenamed('descricao', 'desc_situacao_cadastral')
                    # )
                    # institutions = institutions.join(data, on='cod_situacao_cadastral')

                    institutions = institutions.replace(np_nan, None)
                    institutions = institutions.replace('NaN', None)
                    institutions = institutions.replace('NAN', None)

                    institutions = institutions.toPandas()
                    self.file.save(institutions, parquet_path)
                    self.log.info(f'{parquet_name} done!.')
            else:
                self.log.info(f'{folder_name} already exists.')
            self.log.info(f'{folder_name.split("_")[0]} done!')
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
            self.institutions()
            # self.companies()
        except Exception:
            raise

        self.log.info('----- Transform -----')
