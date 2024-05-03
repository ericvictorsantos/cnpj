# coding: utf-8
""" Local Database """

# built-in

# installed
from duckdb import connect as duckdb_connect

# custom


class LocalBase:
    """
    Local database class.

    Attributes
    ----------

    Methods
    -------

    """

    __conn = None

    def __init__(self, config):
        self.threads = config['params']['duck_threads']
        self.max_memory = config['params']['duck_max_memory']

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

        if LocalBase.__conn:
            LocalBase.__conn.close()

    def open_database_connection(self):
        """
        Open database connection.

        Parameters
        ----------
        None.

        Returns
        -------
        conn : duckdb.DuckDBPyConnection
            Database connection.
        """

        if LocalBase.__conn is None:
            config = {'threads': self.threads, 'max_memory': self.max_memory}
            LocalBase.__conn = duckdb_connect(database=':memory:', config=config)

        return LocalBase.__conn
