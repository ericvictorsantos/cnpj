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
from pandas import read_csv as pd_read_csv

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
            if not self.file.exists(f'silver/{output_file_name}'):
                with ZipFile(file_zip) as obj_zip:
                    input_file_name = obj_zip.filelist[0].filename
                    input_file = obj_zip.open(input_file_name)
                    output_file = pd_read_csv(input_file, sep=';', encoding='latin-1', header=None, names=columns)
                    output_file['descricao'] = output_file['descricao'].str.upper()
                    self.file.save(output_file, f'silver/{output_file_name}')
                    self.log.info(f'{output_file_name} done!.')
            else:
                self.log.info(f'{output_file_name} already exists.')
            self.log.info(f'{zip_name.split("_")[0]} done!')

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

        file_zips = sorted(self.file.files(f'bronze/Est*.zip'))

        for file_zip in file_zips:
            zip_name = os_path.basename(file_zip).split('.')[0]
            self.log.info(f'{zip_name.split("_")[0]}...')
            with ZipFile(file_zip) as obj_zip:
                input_file_name = obj_zip.filelist[0].filename
                input_file = obj_zip.open(input_file_name)
                chunks = pd_read_csv(input_file, sep=';', encoding='latin-1', header=None, dtype=str, names=columns, chunksize=self.chunk_size)
                for idx, chunk in enumerate(chunks, start=1):
                    output_file_name = f'{zip_name}_{str(idx).rjust(2, "0")}.parquet'
                    if not self.file.exists(f'silver/{output_file_name}'):
                        self.file.save(chunk, f'silver/{output_file_name}')
                        self.log.info(f'{output_file_name} done!.')
                    else:
                        self.log.info(f'{output_file_name} already exists.')
            self.log.info(f'{zip_name.split("_")[0]} done!')

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

        start_time = time()

        self.log.info(f'transform...')

        try:
            self.domains()
            self.institutions()
        except Exception:
            raise

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'transform done! {elapsed_time}s')
