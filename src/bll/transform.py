# coding: utf-8
""" Data Transformation Module """

# built-in
from time import time
from logging import getLogger
from datetime import datetime

# installed
from pandas import DataFrame as pd_DataFrame
from numpy import sum as np_sum, mean as np_mean

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
        self.quantity_clients = self.config['job']['clients']
        self.quantity_products = self.config['job']['products']
        self.date_time = datetime.utcnow()
        self.layer = 'silver'

    def client_potentials(self, dag_id, tenant_name):
        """
        Get client sale potentials.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_name : str
            Tenant name.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('transform_client_potentials...')
        dag_path = f'{tenant_name}/{dag_id}'
        file_name = f'{dag_path}/{self.layer}/client_potentials'
        potentials = self.file.load(file_name)

        if potentials is None:
            # previous orders
            previous_orders = self.file.load(f'{dag_path}/{self.layer}/previous_orders')
            previous_orders = (
                previous_orders
                .groupby(['userExternalCode', 'clientExternalCode', 'month'])
                .agg({'totalValue': np_sum, 'totalQuantity': np_sum, 'totalWeight': np_sum})
                .reset_index()
            )

            previous_orders = (
                previous_orders
                .groupby(['userExternalCode', 'clientExternalCode'])
                .agg({'totalValue': np_mean, 'totalQuantity': np_mean, 'totalWeight': np_mean})
                .reset_index()
            )

            previous_orders = (
                previous_orders
                .rename(columns={'totalValue': 'meanValue', 'totalQuantity': 'meanQuantity',
                                 'totalWeight': 'meanWeight'})
            )
            previous_orders = (
                previous_orders
                .astype({'meanValue': float, 'meanQuantity': float, 'meanWeight': float})
            )

            # current orders
            current_orders = self.file.load(f'{dag_path}/{self.layer}/current_orders')
            current_orders = (
                current_orders
                .groupby(['userExternalCode', 'clientExternalCode'])
                .agg({'totalValue': np_sum, 'totalQuantity': np_sum, 'totalWeight': np_sum})
                .reset_index()
            )

            current_orders = (
                current_orders
                .rename(columns={'totalValue': 'sumValue', 'totalQuantity': 'sumQuantity',
                                 'totalWeight': 'sumWeight'})
            )
            current_orders = (
                current_orders
                .astype({'sumValue': float, 'sumQuantity': float, 'sumWeight': float})
            )

            # merge
            potentials = (
                previous_orders
                .merge(current_orders,
                       on=['userExternalCode', 'clientExternalCode'], how='left')
            )

            potentials.fillna({'sumValue': 0.0, 'sumQuantity': 0.0, 'sumWeight': 0.0}, inplace=True)

            for suffix in ['Value', 'Quantity', 'Weight']:
                potentials[f'potential{suffix}'] = potentials[f'mean{suffix}'] - potentials[f'sum{suffix}']
                potentials = potentials.round({f'mean{suffix}': 2, f'sum{suffix}': 2, f'potential{suffix}': 2})

            if potentials is None or len(potentials) == 0:
                potentials = pd_DataFrame(columns=['userExternalCode', 'clientExternalCode', 'meanValue', 'meanQuantity', 'meanWeight', 'sumValue', 'sumQuantity', 'sumWeight', 'potentialValue', 'potentialQuantity', 'potentialWeight'])

            self.file.save(potentials, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'client_potentials: {len(potentials)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'transform_client_potentials done! {elapsed_time}s')

    def clients(self, dag_id, tenant_id):
        """
        Transform clients.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_id : str
            Tenant name.

        Returns
        -------
        orders : pandas.DataFrame
            Dataset of orders.
        """

        start_time = time()
        self.log.info('transform_clients...')
        dag_path = f'{tenant_id}/{dag_id}'
        file_name = f'{dag_path}/{self.layer}/clients'
        clients = self.file.load(file_name)

        if clients is None:
            clients = self.file.load(f'{dag_path}/bronze/clients')
            if len(clients) > 0:
                clients = pd_DataFrame(clients)
                clients.rename(columns={'externalCode': 'clientExternalCode', 'name': 'clientName'}, inplace=True)
            else:
                clients = pd_DataFrame(columns=['clientExternalCode', 'clientName'])

            self.file.save(clients, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'clients: {len(clients)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'transform_clients done! {elapsed_time}s')

    def current_orders(self, dag_id, tenant_id):
        """
        Transform current orders.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_id : str
            Tenant name.

        Returns
        -------
        orders : pandas.DataFrame
            Dataset of orders.
        """

        start_time = time()
        self.log.info('transform_current_orders...')
        dag_path = f'{tenant_id}/{dag_id}'
        file_name = f'{dag_path}/{self.layer}/current_orders'
        orders = self.file.load(file_name)

        if orders is None:
            orders = self.file.load(f'{dag_path}/{self.layer}/prepare_orders')
            if len(orders) > 0:
                first_day = datetime(self.date_time.year, self.date_time.month, 1)
                orders = orders[orders['creationDate'] >= first_day]
                orders.drop(columns=['creationDate'], inplace=True)
            else:
                orders = pd_DataFrame(columns=['userExternalCode', 'productExternalCode', 'clientExternalCode',
                                               'month', 'totalValue', 'totalQuantity', 'totalWeight'])

            self.file.save(orders, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'current_orders: {len(orders)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'transform_current_orders done! {elapsed_time}s')

    def orders(self, dag_id, tenant_id):
        """
        Transform orders.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_id : str
            Tenant name.

        Returns
        -------
        orders : pandas.DataFrame
            Dataset of orders.
        """

        start_time = time()
        self.log.info('transform_orders...')
        dag_path = f'{tenant_id}/{dag_id}'
        file_name = f'{dag_path}/{self.layer}/orders'
        orders = self.file.load(file_name)

        if orders is None:
            orders = self.file.load(f'{dag_path}/bronze/orders')
            if len(orders) > 0:
                temp = []
                for order in orders:
                    order['userExternalCodeHeader'] = order.pop('userExternalCode')
                    order_items = order.pop('orderItems')
                    order_details = order.pop('orderDetails')
                    for order_item in order_items:
                        temp.append({**order, **order_details, **order_item})

                orders = pd_DataFrame(temp)
                temp.clear()
                orders['userExternalCode'].fillna(orders['userExternalCodeHeader'], inplace=True)
                orders.drop(columns=['userExternalCodeHeader'], inplace=True)

                wallets = self.file.load(f'{dag_path}/{self.layer}/wallets')
                orders = orders.merge(wallets, on=['userExternalCode', 'clientExternalCode'])

            if orders is None or len(orders) == 0:
                orders = pd_DataFrame(columns=['userExternalCode', 'clientExternalCode', 'creationDate',
                                               'productExternalCode', 'soldPrice', 'soldQuantity'])

            self.file.save(orders, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'orders: {len(orders)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'transform_orders done! {elapsed_time}s')

    def potentials(self, dag_id, tenant_id):
        """
        Join potentials.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_id : str
            Tenant name.

        Returns
        -------
        orders : pandas.DataFrame
            Dataset of orders.
        """

        start_time = time()
        self.log.info('join_potentials...')
        dag_path = f'{tenant_id}/{dag_id}'
        file_name = f'{dag_path}/gold/potentials'
        potentials = self.file.load(file_name)

        if potentials is None:

            if potentials is None or len(potentials) == 0:
                users = self.file.load(f'{dag_path}/{self.layer}/users')
                clients = self.file.load(f'{dag_path}/{self.layer}/clients')
                orders = self.file.load(f'{dag_path}/{self.layer}/orders')
                products = self.file.load(f'{dag_path}/{self.layer}/products')
                client_potentials = self.file.load(f'{dag_path}/{self.layer}/client_potentials')
                product_potentials = self.file.load(f'{dag_path}/{self.layer}/product_potentials')
                previous_potentials = self.file.load(f'{dag_path}/{self.layer}/previous_potentials')

                # products
                potentials = orders[['userExternalCode', 'clientExternalCode']].drop_duplicates()
                default_columns = ['userExternalCode', 'clientExternalCode', 'productExternalCode', 'productName']

                product_potentials = (
                    product_potentials
                    .merge(products[['productExternalCode', 'productName']], on='productExternalCode')
                )

                # fields
                for by in ['Value', 'Quantity', 'Weight']:
                    field = by.lower()
                    columns = [f'mean{by}', f'sum{by}', f'potential{by}']
                    field_potentials = product_potentials[default_columns + columns]
                    product_potentials.drop(columns=columns, inplace=True)
                    field_potentials.columns = field_potentials.columns.str.replace(by, '')
                    field_potentials = field_potentials[field_potentials['potential'] > 0]

                    previous_potentials = (
                        previous_potentials
                        .rename(columns={f'productExternalCode{by}': 'productExternalCode'})
                    )
                    field_potentials = (
                        field_potentials
                        .merge(previous_potentials[['userExternalCode', 'clientExternalCode', 'productExternalCode', 'isPotential']],
                               on=['userExternalCode', 'clientExternalCode', 'productExternalCode'], how='left')
                    )
                    previous_potentials.drop(columns=['productExternalCode'], inplace=True)
                    field_potentials = field_potentials[field_potentials['isPotential'].isna()]
                    field_potentials.drop(columns=['isPotential'], inplace=True)

                    columns = ['productExternalCode', 'productName', 'mean', 'sum', 'potential']
                    field_potentials[field] = field_potentials[columns].to_dict(orient='records')
                    field_potentials.drop(columns=columns, inplace=True)
                    field_potentials = (
                        field_potentials
                        .groupby(['userExternalCode', 'clientExternalCode'])[field]
                        .apply(list)
                        .reset_index()
                    )

                    for idx, temp in field_potentials[field].items():
                        temp = sorted(temp, key=lambda dct: -dct['potential'])[:self.quantity_products]
                        field_potentials.at[idx, field] = temp

                    potentials = (
                        potentials
                        .merge(field_potentials, on=['userExternalCode', 'clientExternalCode'], how='left')
                    )

                columns = ['value', 'quantity', 'weight']
                potentials.dropna(subset=columns, how='all', inplace=True)
                for column in columns:
                    potentials[column] = potentials[column].fillna('').apply(list)

                product_potentials = potentials.copy()

                # clients
                potentials = orders[['userExternalCode']].drop_duplicates()
                default_columns = ['userExternalCode', 'clientExternalCode', 'clientName']
                client_potentials = client_potentials.merge(clients, on='clientExternalCode')

                # fields
                for by in ['Value', 'Quantity', 'Weight']:
                    field = by.lower()
                    columns = [f'mean{by}', f'sum{by}', f'potential{by}']
                    field_potentials = client_potentials[default_columns + columns]
                    client_potentials.drop(columns=columns, inplace=True)
                    field_potentials.columns = field_potentials.columns.str.replace(by, '')
                    field_potentials = field_potentials[field_potentials['potential'] > 0]

                    field_products_potential = product_potentials[['userExternalCode', 'clientExternalCode', field]]
                    product_potentials.drop(columns=[field], inplace=True)
                    field_potentials = (
                        field_potentials
                        .merge(field_products_potential, on=['userExternalCode', 'clientExternalCode'])
                    )
                    field_potentials.rename(columns={field: 'products'}, inplace=True)

                    columns = ['clientExternalCode', 'clientName', 'mean', 'sum', 'potential', 'products']
                    field_potentials[field] = field_potentials[columns].to_dict(orient='records')
                    field_potentials.drop(columns=columns, inplace=True)
                    field_potentials = (
                        field_potentials
                        .groupby(['userExternalCode'])[field]
                        .apply(list)
                        .reset_index()
                    )

                    for idx, temp in field_potentials[field].items():
                        temp = sorted(temp, key=lambda dct: -dct['potential'])[:self.quantity_clients]
                        field_potentials.at[idx, field] = temp

                    potentials = potentials.merge(field_potentials, on=['userExternalCode'], how='left')

                columns = ['value', 'quantity', 'weight']
                potentials.dropna(subset=columns, how='all', inplace=True)
                for column in columns:
                    potentials[column] = potentials[column].fillna('').apply(list)

                potentials['potentials'] = potentials[['value', 'quantity', 'weight']].to_dict(orient='records')
                potentials = potentials[['userExternalCode', 'potentials']]

                potentials = potentials.merge(users, on='userExternalCode')

                potentials["isActive"] = True
                potentials['achievedGoal'] = False
                potentials["createdAt"] = self.date_time

                if potentials is None or len(potentials) == 0:
                    potentials = pd_DataFrame(columns=['userExternalCode', 'userName', 'potentials', 'achievedGoal',
                                                       'createdAt', 'isActive'])

            self.file.save(potentials, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'potentials: {len(potentials)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'join_potentials done! {elapsed_time}s')

    def prepare_orders(self, dag_id, tenant_id):
        """
        Transform orders.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_id : str
            Tenant name.

        Returns
        -------
        orders : pandas.DataFrame
            Dataset of orders.
        """

        start_time = time()
        self.log.info('transform_prepare_orders...')
        dag_path = f'{tenant_id}/{dag_id}'
        file_name = f'{dag_path}/{self.layer}/prepare_orders'
        orders = self.file.load(file_name)

        if orders is None:
            orders = self.file.load(f'{dag_path}/{self.layer}/orders')
            if len(orders) > 0:
                products = self.file.load(f'{dag_path}/{self.layer}/products')
                orders = orders.merge(products[['productExternalCode', 'netWeight']], on='productExternalCode')

                orders['totalValue'] = orders['soldQuantity'] * orders['soldPrice']
                orders['totalWeight'] = orders['soldQuantity'] * orders['netWeight']

                orders.rename(columns={'soldQuantity': 'totalQuantity'}, inplace=True)
                orders.drop(columns=['soldPrice', 'netWeight'], inplace=True)

            if orders is None or len(orders) == 0:
                orders = pd_DataFrame(columns=['userExternalCode', 'clientExternalCode', 'creationDate',
                                               'productExternalCode', 'totalValue', 'totalQuantity', 'totalWeight'])

            self.file.save(orders, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'orders: {len(orders)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'transform_prepare_orders done! {elapsed_time}s')

    def previous_orders(self, dag_id, tenant_id):
        """
        Transform previous orders.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_id : str
            Tenant name.

        Returns
        -------
        orders : pandas.DataFrame
            Dataset of orders.
        """

        start_time = time()
        self.log.info('transform_previous_orders...')
        dag_path = f'{tenant_id}/{dag_id}'
        file_name = f'{dag_path}/{self.layer}/previous_orders'
        orders = self.file.load(file_name)

        if orders is None:
            orders = self.file.load(f'{dag_path}/{self.layer}/prepare_orders')
            if len(orders) > 0:
                first_day = datetime(self.date_time.year, self.date_time.month, 1)
                orders = orders[orders['creationDate'] < first_day]
                orders.rename(columns={'creationDate': 'month'}, inplace=True)
                orders['month'] = [int(date.strftime("%Y%m")) for date in orders['month']]
            else:
                orders = pd_DataFrame(columns=['userExternalCode', 'productExternalCode', 'clientExternalCode',
                                               'month', 'totalValue', 'totalQuantity',  'totalWeight'])

            self.file.save(orders, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'previous_orders: {len(orders)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'transform_previous_orders done! {elapsed_time}s')

    def previous_potentials(self, dag_id, tenant_id):
        """
        Transform previous_potentials.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_id : str
            Tenant name.

        Returns
        -------
        orders : pandas.DataFrame
            Dataset of orders.
        """

        start_time = time()
        self.log.info('transform_previous_potentials...')
        dag_path = f'{tenant_id}/{dag_id}'
        file_name = f'{dag_path}/{self.layer}/previous_potentials'
        potentials = self.file.load(file_name)

        if potentials is None:
            potentials = self.file.load(f'{dag_path}/bronze/previous_potentials')
            if len(potentials) > 0:
                temp = []
                for potential in potentials:
                    user_code = potential.pop('userExternalCode')
                    fields = potential.pop('potentials', {})
                    for field, clients in fields.items():
                        for client in clients:
                            client_code = client.pop('clientExternalCode')
                            products = client.pop('products')
                            for product in products:
                                product_code = product.pop('productExternalCode')
                                temp.append({'userExternalCode': user_code,
                                             'clientExternalCode': client_code,
                                             f'productExternalCode{field.capitalize()}': product_code})

                potentials = pd_DataFrame(temp)
                temp.clear()
                potentials['isPotential'] = True

            if potentials is None or len(potentials) == 0:
                potentials = pd_DataFrame(
                    columns=['userExternalCode', 'clientExternalCode', 'productExternalCodeValue',
                             'productExternalCodeQuantity', 'productExternalCodeWeight', 'isPotential'])

            self.file.save(potentials, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'previous_potentials: {len(potentials)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'transform_previous_potentials done! {elapsed_time}s')

    def product_potentials(self, dag_id, tenant_name):
        """
        Get product sale potentials.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_name : str
            Tenant name.

        Returns
        -------
        None
        """

        start_time = time()
        self.log.info('transform_product_potentials...')
        dag_path = f'{tenant_name}/{dag_id}'
        file_name = f'{dag_path}/{self.layer}/product_potentials'
        potentials = self.file.load(file_name)

        if potentials is None:
            # previous orders
            previous_orders = self.file.load(f'{dag_path}/{self.layer}/previous_orders')
            previous_orders = (
                previous_orders
                .groupby(['userExternalCode', 'clientExternalCode', 'productExternalCode', 'month'])
                .agg({'totalValue': np_sum, 'totalQuantity': np_sum, 'totalWeight': np_sum})
                .reset_index()
            )

            previous_orders = (
                previous_orders
                .groupby(['userExternalCode', 'clientExternalCode', 'productExternalCode'])
                .agg({'totalValue': np_mean, 'totalQuantity': np_mean, 'totalWeight': np_mean})
                .reset_index()
            )

            previous_orders = (
                previous_orders
                .rename(columns={'totalValue': 'meanValue', 'totalQuantity': 'meanQuantity',
                                 'totalWeight': 'meanWeight'})
            )
            previous_orders = (
                previous_orders
                .astype({'meanValue': float, 'meanQuantity': float, 'meanWeight': float})
            )

            # current orders
            current_orders = self.file.load(f'{dag_path}/{self.layer}/current_orders')
            current_orders = (
                current_orders
                .groupby(['userExternalCode', 'clientExternalCode', 'productExternalCode'])
                .agg({'totalValue': np_sum, 'totalQuantity': np_sum, 'totalWeight': np_sum})
                .reset_index()
            )

            current_orders = (
                current_orders
                .rename(columns={'totalValue': 'sumValue', 'totalQuantity': 'sumQuantity',
                                 'totalWeight': 'sumWeight'})
            )
            current_orders = (
                current_orders
                .astype({'sumValue': float, 'sumQuantity': float, 'sumWeight': float})
            )

            # merge
            potentials = (
                previous_orders
                .merge(current_orders,
                       on=['userExternalCode', 'clientExternalCode', 'productExternalCode'], how='left')
            )

            potentials = potentials.fillna({'sumValue': 0.0, 'sumQuantity': 0.0, 'sumWeight': 0.0})

            for suffix in ['Value', 'Quantity', 'Weight']:
                potentials[f'potential{suffix}'] = potentials[f'mean{suffix}'] - potentials[f'sum{suffix}']
                potentials = potentials.round({f'mean{suffix}': 2, f'sum{suffix}': 2, f'potential{suffix}': 2})

            if potentials is None or len(potentials) == 0:
                potentials = (
                    pd_DataFrame(columns=['userExternalCode', 'clientExternalCode', 'productExternalCode', 'meanValue', 'meanQuantity', 'meanWeight', 'sumValue', 'sumQuantity', 'sumWeight', 'potentialValue', 'potentialQuantity', 'potentialWeight'])
                )

            self.file.save(potentials, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'product_potentials: {len(potentials)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'transform_product_potentials done! {elapsed_time}s')

    def products(self, dag_id, tenant_id):
        """
        Transform products.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_id : str
            Tenant name.

        Returns
        -------
        None.
        """

        start_time = time()
        self.log.info('transform_products...')
        dag_path = f'{tenant_id}/{dag_id}'
        file_name = f'{dag_path}/{self.layer}/products'
        products = self.file.load(file_name)

        if products is None:
            products = self.file.load(f'{dag_path}/bronze/products')
            if len(products) > 0:
                products = pd_DataFrame(products)
                products.rename(columns={'externalCode': 'productExternalCode', 'name': 'productName'}, inplace=True)
            else:
                products = pd_DataFrame(columns=['productExternalCode', 'productName', 'netWeight'])

            self.file.save(products, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'products: {len(products)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'transform_products done! {elapsed_time}s')

    def users(self, dag_id, tenant_id):
        """
        Transform users.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_id : str
            Tenant name.

        Returns
        -------
        None.
        """

        start_time = time()
        self.log.info('transform_users...')
        dag_path = f'{tenant_id}/{dag_id}'
        file_name = f'{dag_path}/{self.layer}/users'
        users = self.file.load(file_name)

        if users is None:
            users = self.file.load(f'{dag_path}/bronze/users')
            if len(users) > 0:
                users = pd_DataFrame(users)
                users.rename(columns={'externalCode': 'userExternalCode'}, inplace=True)
            else:
                users = pd_DataFrame(columns=['userExternalCode', 'userName'])

            self.file.save(users, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'users: {len(users)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'transform_users done! {elapsed_time}s')

    def wallets(self, dag_id, tenant_id):
        """
        Transform wallets.

        Parameters
        ----------
        dag_id : str
            DAG id.
        tenant_id : str
            Tenant name.

        Returns
        -------
        orders : pandas.DataFrame
            Dataset of orders.
        """

        start_time = time()
        self.log.info('transform_wallets...')
        dag_path = f'{tenant_id}/{dag_id}'
        file_name = f'{dag_path}/{self.layer}/wallets'
        wallets = self.file.load(file_name)

        if wallets is None:
            wallets = self.file.load(f'{dag_path}/bronze/wallets')
            if len(wallets) > 0:
                wallets = pd_DataFrame(wallets)
                wallets.rename(columns={'clientsExternalCode': 'clientExternalCode'}, inplace=True)
                wallets = wallets.explode('clientExternalCode')
            else:
                wallets = pd_DataFrame(columns=['userExternalCode', 'clientExternalCode'])

            self.file.save(wallets, file_name)
        else:
            self.log.info('using cache.')

        self.log.info(f'wallets: {len(wallets)}')

        elapsed_time = round(time() - start_time, 3)
        self.log.info(f'transform_wallets done! {elapsed_time}s')
