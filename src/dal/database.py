# coding: utf-8
""" Tenant Database """

# built-in

# installed
from psycopg2 import extras
from airflow.providers.postgres.hooks.postgres import PostgresHook

# custom
from cnpj.config import Config


class CNPJBase:
    """
    Class CNPJ database.

    Attributes
    ----------

    Methods
    -------

    """

    conn = None

    def __init__(self, config):
        self.chunk_size = config['job']['chunk_size']

    @staticmethod
    def close_database_connection():
        """
        Close database connection.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        if CNPJBase.conn:
            CNPJBase.conn.close()

    def get_imported_files(self):
        """
        Get imported files.

        Parameters
        ----------
        None.

        Returns
        -------
        imported_files : list[str]
            Imported files.
        """

        query = f'''select nome_arquivo from arquivo;'''

        try:
            conn = self.open_database_connection()
            with conn.cursor() as cursor:
                cursor.execute(query)
                imported_files = cursor.fetchall()
        except Exception:
            raise

        return imported_files

    @staticmethod
    def open_database_connection():
        """
        Open database connection.

        Parameters
        ----------
        None.

        Returns
        -------
        conn : pymongo.MongoClient
            Database client.
        """

        if CNPJBase.conn is None:
            CNPJBase.conn = PostgresHook(postgres_conn_id='cnpj').get_conn()
            CNPJBase.conn.autocommit = True

        conn = CNPJBase.conn

        return conn

    def set_file_info(self, file_info):
        """
        Set wallets.

        Parameters
        ----------
        file_info : list[dict]
            List of data.

        Returns
        -------
        None
        """

        query = []
        fields = list(file_info[0].keys())
        columns = [f'"{column}"' for column in fields]

        # insert
        field_names = ', '.join([f'%({field})s' for field in fields])
        column_names = str(columns).replace('[', '(').replace(']', ')').replace("'", "")
        query.append(f'''insert into "arquivo" {column_names} values ({field_names})''')

        # update
        query.append(f'''on conflict ("nome")''')
        set_values = ', '.join(f'{column} = %({field})s' for column, field in zip(columns, fields))
        query.append(f"do update set {set_values};")
        query = ' '.join(query)

        try:
            conn = self.open_database_connection()
            with conn.cursor() as cursor:
                extras.execute_batch(cursor, query, file_info)
        except Exception:
            raise

    def set_domains(self, table, values):
        """
        Set wallets.

        Parameters
        ----------
        table : str
            Table name.
        values : list[dict]
            List of data.

        Returns
        -------
        None
        """

        query = []
        fields = list(values[0].keys())
        columns = [f'"{column}"' for column in fields]
        limit = 1000

        # insert
        field_names = ', '.join([f'%({field})s' for field in fields])
        column_names = str(columns).replace('[', '(').replace(']', ')').replace("'", "")
        query.append(f'''insert into {table} values ({field_names})''')

        # update
        query.append(f'''on conflict ("codigo")''')
        set_values = ', '.join(f'{column} = %({field})s' for column, field in zip(columns, fields))
        query.append(f"do update set {set_values};")
        query = ' '.join(query)

        try:
            conn = self.open_database_connection()
            with conn.cursor() as cursor:
                for idx in range(0, len(values), limit):
                    chunk_values = values[idx: idx + limit]
                    extras.execute_batch(cursor, query, chunk_values)
        except Exception:
            raise

    def upsert_companies(self):
        """
        Set companies.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        query = '''
            insert into
                empresa
            select
                *
            from
                empresa_temp
            on conflict
                (cnpj_basico)
            do update set
                razao_social = excluded.razao_social,
                cod_natureza_juridica = excluded.cod_natureza_juridica,
                cod_qualificacao_responsavel = excluded.cod_qualificacao_responsavel,
                capital_social = excluded.capital_social,
                cod_porte_empresa = excluded.capital_social,
                atualizado_em = excluded.atualizado_em;
            drop table empresa_temp;
        '''

        try:
            conn = self.open_database_connection()
            with conn.cursor() as cursor:
                cursor.execute(query)
        except Exception:
            raise

    def upsert_institutions(self):
        """
        Upsert institutions.

        Parameters
        ----------
        None.

        Returns
        -------
        None
        """

        query = '''
            insert into
                estabelecimento 
            select
                *
            from
                estabelecimento_temp
            on conflict
                (cnpj_completo)
            do update set
                cod_matriz_filial = excluded.cod_matriz_filial,
                nome_fantasia = excluded.nome_fantasia,
                cod_situacao_cadastral = excluded.cod_situacao_cadastral,
                data_situacao_cadastral = excluded.data_situacao_cadastral,
                cod_motivo_situacao_cadastral = excluded.cod_motivo_situacao_cadastral,
                nome_cidade_exterior = excluded.nome_cidade_exterior,
                cod_pais = excluded.cod_pais,
                data_inicio_atividade = excluded.data_inicio_atividade,
                cod_cnae_principal = excluded.cod_cnae_principal,
                cod_cnae_secundaria = excluded.cod_cnae_secundaria,
                tipo_logradouro = excluded.tipo_logradouro,
                logradouro = excluded.logradouro,
                numero = excluded.numero,
                complemento = excluded.complemento,
                bairro = excluded.bairro,
                cep = excluded.cep,
                uf = excluded.uf,
                cod_municipio = excluded.cod_municipio,
                ddd_1 = excluded.ddd_1,
                telefone_1 = excluded.telefone_1,
                ddd_2 = excluded.ddd_2,
                telefone_2 = excluded.telefone_2,
                ddd_fax = excluded.ddd_fax,
                fax = excluded.fax,
                correio_eletronico = excluded.correio_eletronico,
                situacao_especial = excluded.situacao_especial,
                data_situacao_especial = excluded.data_situacao_especial,
                cnpj_completo = excluded.cnpj_completo,
                atualizado_em = excluded.atualizado_em;
            drop table estabelecimento_temp;
        '''

        try:
            conn = self.open_database_connection()
            with conn.cursor() as cursor:
                cursor.execute(query)
        except Exception:
            raise
