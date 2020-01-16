import ssl
from impala.dbapi import connect
import os

os.system("kinit")

def impalacon(host_name, port_no, ssl_use, database_name, user_name, kerberos_svc_name, auth_mech):

	conn = connect(host=host_name,
              	port=port_no,
              	use_ssl=ssl_use,
              	database=database_name,
              	user=user_name,
              	kerberos_service_name=kerberos_svc_name,
              	auth_mechanism = auth_mech)
	
	return conn.cursor()


def find_table_across_alldb(impala_cur, table_name):
	query = show tables like table_name;
	impala_cur.execute(query)
	result = impala_cur.fetchall()
	return result

def check_table_properties(impala_cur, table_name):
	impala_cur.execute('show databases like sumit*;')
	result = impala_cur.fetchall()
	return result

def main():
	cur = impalacon("worker2.cluster.io", 21050, True, "default", "sumit", "impala", "GSSAPI")
	print (find_table_across_alldb(cur, "test*"))
	print (check_table_properties(cur, 'test*'))

if __name__ == "__main__":
	main()
	
