# coding: utf-8
""" Data Transformation Module """

# built-in
from time import time
from pathlib import Path
from zipfile import ZipFile
from logging import getLogger
from re import compile as re_compile
from os import path as os_path
from pathlib import Path

# installed
from numpy import nan as np_nan
from pandas import read_csv as pd_read_csv, DataFrame as pd_DataFrame
from pyspark.sql import SparkSession, functions as f, types as sql_types

# custom
from cnpj.src.dal.file import File
from cnpj.src.dal.local import LocalBase


class Transform:
    """
    Data transformation Class

    Attributes
    ----------

    Methods
    -------
    run_job()
        Execute job.
    """

    def __init__(self, config):
        self.file = File(config)
        self.local_base = LocalBase(config)
        self.log = getLogger('airflow.task')
        self.layer = 'silver'
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

        site_files = self.file.load('bronze/site_files.bin')
        site_files = site_files['nome_arquivo'].tolist()
        site_files = [Path(site_file).stem for site_file in site_files if site_file.startswith('Emp')]

        self.log.info('----- Bronze -> Silver -----')
        local_files = sorted(self.file.files(f'{self.layer}/Emp*'))
        local_files = [Path(file).stem for file in local_files]

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'{self.layer}/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'companies files: {len(files)}')

        for input_file in files:
            self.log.info(f'{input_file}...')
            output_file = f'{input_file}.csv'
            if not self.file.exists(f'{self.layer}/{output_file}'):
                with ZipFile(f'{self.data_path}/bronze/{input_file}.zip') as input_context:
                    with open(f'{self.data_path}/{self.layer}/{output_file}', mode='w') as output_context:
                        csv_name = input_context.filelist[0].filename
                        csv = input_context.open(csv_name)
                        for line in csv:
                            output_context.write(line.decode('latin-1'))
            else:
                self.log.info(f'{output_file} already exists.')
            self.log.info(f'{input_file} done!')
        self.log.info('----- Bronze -> Silver -----')

        self.log.info('----- Silver -> Gold -----')
        local_files = sorted(self.file.files(f'gold/Emp*'))
        local_files = [Path(file).stem for file in local_files]

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'gold/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'companies files: {len(files)}')

        for input_file in files:
            self.log.info(f'{input_file}...')
            output_file = f'{input_file}.parquet'
            query = '''
                copy (
                    select
                        cast("column0" as integer) as "cnpj_basico",
                        regexp_replace("column1", '\s{{2,}}', ' ') as "razao_social",
                        cast("column2" as integer) as "cod_natureza_juridica",
                        cast("column3" as integer) as "cod_qualificacao_responsavel",
                        cast(replace("column4", ',', '.') as float) as "capital_social",
                        cast("column5" as integer) as "cod_porte_empresa",
                        "column6" as "ente_federativo",
                        current_timestamp as "atualizado_em"
                    from
                        read_csv('{input}', header=False, all_varchar=true)
                )
                to '{output}'
                (format 'parquet', compression 'snappy', row_group_size 100_000);
            '''

            kwargs = {
                'input': f'{self.data_path}/{self.layer}/{input_file}.csv',
                'output': f'{self.data_path}/gold/{output_file}'
            }

            conn = self.local_base.open_database_connection()
            conn.execute(query.format(**kwargs))
            self.log.info(f'{input_file} done!')
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
        site_files = [Path(file).stem for file in site_files if re_domains.search(file)]

        self.log.info('----- Bronze -> Silver -----')
        local_files = sorted(self.file.files(f'silver/*.csv'))
        local_files = [Path(file).stem for file in local_files if re_domains.search(file)]

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'{self.layer}/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'companies files: {len(files)}')

        for input_file in files:
            self.log.info(f'{input_file}...')
            output_file = f'{input_file}.csv'
            if not self.file.exists(f'{self.layer}/{output_file}'):
                with ZipFile(f'{self.data_path}/bronze/{input_file}.zip') as input_context:
                    with open(f'{self.data_path}/{self.layer}/{output_file}', mode='w') as output_context:
                        csv_name = input_context.filelist[0].filename
                        csv = input_context.open(csv_name)
                        for line in csv:
                            output_context.write(line.decode('latin-1'))
            else:
                self.log.info(f'{output_file} already exists.')
            self.log.info(f'{input_file} done!')
        self.log.info('----- Bronze -> Silver -----')

        self.log.info('----- Silver -> Gold -----')
        local_files = sorted(self.file.files(f'gold/*.parquet'))
        local_files = [Path(file).stem for file in local_files if re_domains.search(file)]

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'{self.layer}/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'companies files: {len(files)}')

        for input_file in files:
            self.log.info(f'{input_file}...')
            output_file = f'{input_file}.parquet'
            if not self.file.exists(f'gold/{output_file}'):
                query = '''
                    copy (
                        select
                            cast(column0 as integer) as "codigo",
                            cast(upper(column1) as varchar) as "descricao",
                        from
                            read_csv('{input}', header=False)
                    )
                    to '{output}'
                    (format 'parquet', compression 'snappy');
                '''

                kwargs = {
                    'input': f'{self.data_path}/{self.layer}/{input_file}.csv',
                    'output': f'{self.data_path}/gold/{output_file}'
                }

                conn = self.local_base.open_database_connection()
                conn.execute(query.format(**kwargs))
            else:
                self.log.info(f'{output_file} already exists.')
            self.log.info(f'{input_file} done!')

        query = '''
            copy (
                select
                    *
                from
                    {input}
            )
            to '{output}'
            (format 'parquet', compression 'snappy');
        '''

        # porte empresa
        file_name = 'Porte_' + site_files[0].rsplit('_')[-1]
        self.log.info(f'{file_name}...')
        output_file = f'{file_name}.parquet'
        if not self.file.exists(f'gold/{output_file}'):
            data = {
                'codigo': [0, 1, 3, 5],
                'descricao': ['NÃO INFORMADO', 'MICRO EMPRESA', 'EMPRESA DE PEQUENO PORTE', 'DEMAIS']
            }
            data = pd_DataFrame(data)

            kwargs = {
                'input': 'data',
                'output': f'{self.data_path}/gold/{output_file}'
            }

            conn = self.local_base.open_database_connection()
            conn.execute(query.format(**kwargs))
            self.log.info(f'{file_name} done!')
        else:
            self.log.info(f'{file_name} already exists.')

        # matriz filial
        file_name = 'Matriz_' + site_files[0].rsplit('_')[-1]
        self.log.info(f'{file_name}...')
        output_file = f'{file_name}.parquet'
        if not self.file.exists(f'gold/{output_file}'):
            data = {
                'codigo': [1, 2],
                'descricao': ['MATRIZ', 'FILIAL']

            }
            data = pd_DataFrame(data)

            kwargs = {
                'input': 'data',
                'output': f'{self.data_path}/gold/{output_file}'
            }

            conn = self.local_base.open_database_connection()
            conn.execute(query.format(**kwargs))
            self.log.info(f'{file_name} done!')
        else:
            self.log.info(f'{file_name} already exists.')

        # situação cadastral
        file_name = 'Situacao_' + site_files[0].rsplit('_')[-1]
        self.log.info(f'{file_name}...')
        file_path = f'{self.layer}/{file_name}.parquet'
        if not self.file.exists(file_path):
            data = {
                'codigo': [1, 2, 3, 4, 8],
                'descricao': ['NULA', 'ATIVA', 'SUSPENSA', 'INAPTA', 'BAIXADA']
            }
            data = pd_DataFrame(data)

            kwargs = {
                'input': 'data',
                'output': f'{self.data_path}/gold/{output_file}'
            }

            conn = self.local_base.open_database_connection()
            conn.execute(query.format(**kwargs))
            self.log.info(f'{file_name} done!')
        else:
            self.log.info(f'{file_name} already exists.')
        self.log.info('----- Silver -> Gold -----')

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

        site_files = self.file.load('bronze/site_files.bin')
        site_files = site_files['nome_arquivo'].tolist()
        site_files = [Path(site_file).stem for site_file in site_files if site_file.startswith('Est')]

        self.log.info('----- Bronze -> Silver -----')
        local_files = sorted(self.file.files(f'{self.layer}/Est*'))
        local_files = [Path(file).stem for file in local_files]

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'{self.layer}/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'institutions files: {len(files)}')

        for input_file in files:
            self.log.info(f'{input_file}...')
            output_file = f'{input_file}.csv'
            if not self.file.exists(f'{self.layer}/{output_file}'):
                with ZipFile(f'{self.data_path}/bronze/{input_file}.zip') as input_context:
                    with open(f'{self.data_path}/{self.layer}/{output_file}', mode='w') as output_context:
                        csv_name = input_context.filelist[0].filename
                        csv = input_context.open(csv_name)
                        for line in csv:
                            output_context.write(line.decode('latin-1'))
            else:
                self.log.info(f'{output_file} already exists.')
            self.log.info(f'{input_file} done!')
        self.log.info('----- Bronze -> Silver -----')

        self.log.info('----- Silver -> Gold -----')
        local_files = sorted(self.file.files(f'gold/Est*'))
        local_files = [Path(file).stem for file in local_files]

        files = sorted(list(set(local_files) - set(site_files)))
        if len(files) > 0:
            files = [f'gold/{file}' for file in files]
            self.file.delete(files)
        self.log.info(f'delete files: {len(files)}')

        files = sorted(list(set(site_files) - set(local_files)))
        self.log.info(f'institutions files: {len(files)}')

        for input_file in files:
            self.log.info(f'{input_file}...')
            output_file = f'{input_file}.parquet'
            query = '''
                copy (
                    select
                        cast("column00" as integer) as "cnpj_basico",
                        concat("column00", "column01", "column02") as "cnpj_completo",
                        cast("column03" as integer) as "cod_matriz_filial",
                        "column04" as "nome_fantasia",
                        cast("column05" as integer) as "cod_situacao_cadastral",
                        case when regexp_matches("column06", '^(19|20)\d{{6}}') then strptime("column06", '%Y%m%d')::DATE else null end as "data_situacao_cadastral",
                        cast("column07" as integer) as "cod_motivo_situacao_cadastral",
                        "column08" as "nome_cidade_exterior",
                        cast("column09" as integer) as "cod_pais",
                        case when regexp_matches("column10", '^(19|20)\d{{6}}') then strptime("column10", '%Y%m%d') else null end as "data_inicio_atividade",
                        cast("column11" as integer) as "cod_cnae_principal",
                        "column12" as "cod_cnae_secundaria",
                        "column13" as "tipo_logradouro",
                        upper(regexp_replace(regexp_replace("column14", '[^a-zA-Z0-9\s]', ' ', 'g'), '\s{{2,}}', ' ')) as "logradouro",
                        "column15" as "numero",
                        upper(regexp_replace(regexp_replace("column16", '[^a-zA-Z0-9\s]', ' ', 'g'), '\s{{2,}}', ' ')) as "complemento",
                        upper("column17") as "bairro",
                        "column18" as "cep",
                        "column19" as "uf",
                        cast("column20" as integer) as "cod_municipio",
                        "column21" as "ddd_1",
                        "column22" as "telefone_1",
                        "column23" as "ddd_2",
                        "column24" as "telefone_2",
                        "column25" as "ddd_fax",
                        "column26" as "fax",
                        upper("column27") as "correio_eletronico",
                        "column28" as "situacao_especial",
                        case when regexp_matches("column29", '^(19|20)\d{{6}}') then strptime("column29", '%Y%m%d')::DATE else null end as "data_situacao_especial",
                        now() as "atualizado_em"
                    from
                        read_csv('{input}', header=False, all_varchar=true)
                )
                to '{output}'
                (format 'parquet', compression 'snappy', row_group_size 100_000);
            '''

            kwargs = {
                'input': f'{self.data_path}/{self.layer}/{input_file}.csv',
                'output': f'{self.data_path}/gold/{output_file}'
            }

            conn = self.local_base.open_database_connection()
            conn.execute(query.format(**kwargs))
            self.log.info(f'{input_file} done!')
        self.log.info('----- Silver -> Gold -----')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'institutions done! {elapsed_time}s')
