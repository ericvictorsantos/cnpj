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

    def __init__(self):
        self.config = Config().load_config()
        self.limit = self.config['job']['limit']

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

    def delete_expired_data(self, tenant_id, expiration_date):
        """
        Delete expired data.

        Parameters
        ----------
        tenant_id : str
            ID of the tenant.
        expiration_date : datetime.datetime
            Date of the expiration.

        Returns
        -------
        deleted : int
            Deleted count.
        """

        conn = self.open_database_connection()
        database = conn[tenant_id]
        collection = database['sales-potential']

        result = collection.delete_many({'createdAt': {'$lt': expiration_date}})
        deleted = result.deleted_count

        return deleted

    def get_clients(self, tenant_id, clients_code):
        """
        Get clients.

        Parameters
        ----------
        tenant_id : str
            ID of the tenant.
        clients_code : list[str]
            List of client external code.

        Returns
        -------
        clients : list[dict]
            List of clients.
        """

        conn = self.open_database_connection()
        database = conn[tenant_id]
        collection = database['client']

        skip = 0
        clients = []

        where = {
            'externalCode': {'$in': clients_code}
        }
        fields = {
            '_id': 0,
            'externalCode': 1,
            'name': 1
        }

        while True:
            cursor = collection.find(where, fields).skip(skip).limit(self.limit)
            results = list(cursor)
            if len(results) == 0:
                break
            clients.extend(results)
            skip += self.limit

        return clients

    def get_orders(self, tenant_id, start_date, clients_code):
        """
        Get orders.

        Parameters
        ----------
        tenant_id : str
            ID of the tenant.
        start_date : datetime
            Date for search.
        clients_code : list[str]
            List of client external code.

        Returns
        -------
        orders : list[dict]
            List of orders.
        """

        conn = self.open_database_connection()
        database = conn[tenant_id]
        collection = database['order']

        skip = 0
        orders = []

        where = {
            'orderDetails.creationDate': {'$gte': start_date},
            'clientExternalCode': {'$in': clients_code},
            'orderStatus': 'billed',
            'orderDetails.orderType': {'$in': ['normal', 'collect', 'futureSale']}
        }

        fields = {
            '_id': 0,
            'userExternalCode': 1,
            'clientExternalCode': 1,
            'orderDetails.creationDate': 1,
            'orderItems.soldPrice': 1,
            'orderItems.soldQuantity': 1,
            'orderItems.productExternalCode': 1,
            'orderItems.userExternalCode': 1
        }

        while True:
            cursor = collection.find(where, fields).skip(skip).limit(self.limit)
            results = list(cursor)
            if len(results) == 0:
                break
            orders.extend(results)
            skip += self.limit

        return orders

    def get_previous_potentials(self, tenant_id, start_date):
        """
        Get potentials.

        Parameters
        ----------
        tenant_id : str
            ID of the tenant.
        start_date : datetime
            Date for search.

        Returns
        -------
        potentials : list[dict]
            List of sales potential.
        """

        conn = self.open_database_connection()
        database = conn[tenant_id]
        collection = database['sales-potential']

        skip = 0
        potentials = []

        where = {
            'createdAt': {'$gte': start_date}
        }
        fields = {
            '_id': 0,
            'userExternalCode': 1,
            'potentials.value.clientExternalCode': 1,
            'potentials.quantity.clientExternalCode': 1,
            'potentials.weight.clientExternalCode': 1,
            'potentials.value.products.productExternalCode': 1,
            'potentials.quantity.products.productExternalCode': 1,
            'potentials.weight.products.productExternalCode': 1,
        }

        while True:
            cursor = collection.find(where, fields).skip(skip).limit(self.limit)
            results = list(cursor)
            if len(results) == 0:
                break
            potentials.extend(results)
            skip += self.limit

        return potentials

    def get_products(self, tenant_id, products_code):
        """
        Get products.

        Parameters
        ----------
        tenant_id : str
            ID of the tenant.
        products_code : list[str]
            List of product external code.

        Returns
        -------
        products : list[dict]
            List of products.
        """

        conn = self.open_database_connection()
        database = conn[tenant_id]
        collection = database['product']

        skip = 0
        products = []

        where = {
            'externalCode': {'$in': products_code}
        }
        fields = {
            '_id': 0,
            'externalCode': 1,
            'name': 1,
            'netWeight': 1
        }

        while True:
            cursor = collection.find(where, fields).skip(skip).limit(self.limit)
            results = list(cursor)
            if len(results) == 0:
                break
            products.extend(results)
            skip += self.limit

        return products

    def get_users(self, tenant_id):
        """
        Get users.

        Parameters
        ----------
        tenant_id : str
            ID of the tenant.

        Returns
        -------
        users : list[dict]
            List of users.
        """

        conn = self.open_database_connection()
        database = conn['e3-base']
        collection = database['user']

        skip = 0
        users = []

        where = {
            'tenantID': tenant_id,
            'inactive': False,
            'userRole': {'$in': ['vendor', 'supervisor']}
        }
        fields = {
            '_id': 0,
            'externalCode': 1,
            'name': 1,
        }

        while True:
            cursor = collection.find(where, fields).skip(skip).limit(self.limit)
            results = list(cursor)
            if len(results) == 0:
                break
            users.extend(results)
            skip += self.limit

        return users

    def get_wallets(self, tenant_id, users_code):
        """
        Get wallets.

        Parameters
        ----------
        tenant_id : str
            ID of the tenant.
        users_code : list[str]
            List of user external code.

        Returns
        -------
        wallets : list[dict]
            List of wallets.
        """

        conn = self.open_database_connection()
        database = conn[tenant_id]
        collection = database['wallet']

        skip = 0
        wallets = []

        where = {
            'userExternalCode': {'$in': users_code},
            'clientsExternalCode.0': {'$exists': True}
        }
        fields = {
            '_id': 0,
            'userExternalCode': 1,
            'clientsExternalCode': 1,
        }

        while True:
            cursor = collection.find(where, fields).skip(skip).limit(self.limit)
            results = list(cursor)
            if len(results) == 0:
                break
            wallets.extend(results)
            skip += self.limit

        return wallets

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
            CNPJBase.conn = PostgresHook(conn_id='cnpj').get_conn()

        conn = CNPJBase.conn

        return conn

    def set_potentials(self, tenant_id, potentials):
        """
        Set products.

        Parameters
        ----------
        tenant_id : str
            ID of the tenant.
        potentials : list[dict]
            List of potentials.

        Returns
        -------
        inserted : int
            Count inserted values.
        updated : int
            Count updated values.
        """

        conn = self.open_database_connection()
        database = conn[tenant_id]
        collection = database['sales-potential']
        inserted = 0
        updated = 0

        updates = []
        for potential in potentials:
            where = {'userExternalCode': potential.get('userExternalCode'),
                     'isActive': potential.get('isActive')}
            update = {'$set': potential}
            updates.append(UpdateOne(where, update))

        for idx in range(0, len(updates), self.limit):
            chunk_updates = updates[idx: idx + self.limit]
            result = collection.bulk_write(chunk_updates)
            inserted += result.upserted_count
            updated += result.modified_count

        return inserted, updated
