# # coding: utf-8
# """ Main Module """
#
# # built-in
# from time import time
# from logging import getLogger
#
# # installed
#
# # custom
# from cnpj.config import Config
# from cnpj.src.bll.end import End
# from cnpj.src.bll.load import Load
# from cnpj.src.bll.start import Start
# from cnpj.src.bll.extract import Extract
# from cnpj.src.bll.transform import Transform
#
#
# class Main:
#     """
#     Class main of job.
#
#     Attributes
#     ----------
#     None.
#
#     Methods
#     -------
#     run_job()
#         Execute job.
#     """
#
#     def __init__(self):
#         self.end = End()
#         self.load = Load()
#         self.start = Start()
#         self.extract = Extract()
#         self.transform = Transform()
#         self.log = getLogger('airflow.task')
#         self.config = Config().load_config()
#         self.local_or_cloud = self.config['environment']['local_or_cloud']
#
#     def run(self, dag_id, tenant_id, company_name):
#         """
#         Job sale potential.
#
#         Parameters
#         ----------
#         dag_id : str
#             ID of DAG
#         tenant_id : str
#             Tenant id.
#         company_name : str
#             Company name.
#
#         Returns
#         -------
#         None
#         """
#
#         start_time = time()
#
#         self.log.info(f'run job...')
#         self.log.info(f'environment: {self.local_or_cloud}')
#         self.log.info(f'companyName: {company_name} ({tenant_id})')
#
#         try:
#             self.start.clear_temporary_data(tenant_id)
#             self.start.open_database_connection()
#             self.extract.users(dag_id, tenant_id)
#             self.transform.users(dag_id, tenant_id)
#             self.extract.wallets(dag_id, tenant_id)
#             self.transform.wallets(dag_id, tenant_id)
#             self.extract.orders(dag_id, tenant_id)
#             self.transform.orders(dag_id, tenant_id)
#             self.extract.clients(dag_id, tenant_id)
#             self.transform.clients(dag_id, tenant_id)
#             self.extract.products(dag_id, tenant_id)
#             self.transform.products(dag_id, tenant_id)
#             self.extract.previous_potentials(dag_id, tenant_id)
#             self.transform.previous_potentials(dag_id, tenant_id)
#             self.transform.prepare_orders(dag_id, tenant_id)
#             self.transform.previous_orders(dag_id, tenant_id)
#             self.transform.current_orders(dag_id, tenant_id)
#             self.transform.product_potentials(dag_id, tenant_id)
#             self.transform.client_potentials(dag_id, tenant_id)
#             self.transform.potentials(dag_id, tenant_id)
#             self.load.potentials(dag_id, tenant_id)
#             self.end.delete_expired_data(tenant_id)
#         except Exception:
#             raise
#         finally:
#             self.end.close_database_connection()
#
#         elapsed_time = round(time() - start_time, 3)
#         self.log.info(f'run job done! {elapsed_time}s')
