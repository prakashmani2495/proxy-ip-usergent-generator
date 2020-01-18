import time
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine


def dbConnection(database, query):
	""" MySql database connection arguments with autocommit transaction """
	try:
		connection = mysql.connector.connect(
			host='localhost',
			user='root',
			passwd='XXXXXXXXXXXXX',
			database=database,
			autocommit=True,
		)
		cursors = connection.cursor()
		cursors.execute(query)
		if "SELECT" in query:
			row = cursors.fetchall()
			if not row:
				cursors.close()
				connection.close()
				return None
			else:
				cursors.close()
				connection.close()
				return row
		else:
			row = cursors.fetchone()
			if row is None:
				cursors.close()
				connection.close()
				return None
			else:
				cursors.close()
				connection.close()
				return row
	except Exception as e:
		print("[{}] Exception Occurs at dbConnection Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
	except KeyboardInterrupt as e:
		print("[{}] Someone Forced Program to EXIT - KeyboardInterrupt at dbConnection Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		exit()


def select(database, table, column='*', condition=None, operator=None):
	""" Select the records from database with flexible conditions """
	sql = list()
	sql.append("SELECT %s " % column)
	sql.append("FROM %s " % table)
	if operator is not None and condition is not None:
		sql.append("WHERE")
		sql.append(operator.join(" %s = '%s' " % (k, v) for k, v in condition.items()))
		sql.append(";")
	elif condition is not None and operator is None:
		sql.append("WHERE")
		sql.append("".join(" %s = '%s' " % (k, v) for k, v in condition.items()))
		sql.append(";")
	else:
		sql.append(";")
	query = "".join(sql)
	db_result = dbConnection(database, query)
	if db_result is not None:
		print("[{}] Select query executed successfully and the Result{} from Database[{}] and Table[{}]."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), db_result, database, table))
		return db_result
	elif db_result is None:
		print(
			"[{}] Select query executed with 'EMPTY SET' Result Set and the Result[{}] from Database[{}] and Table[{}]."
				.format(time.strftime("%I:%M:%S %p", time.localtime()), db_result, database, table))
		return False


def insert(database, table, values):
	""" Insert rows into objects table given the key-value pairs in kwargs """
	key = ["%s" % k for k in values]
	value = ["'%s'" % v for v in values.values()]
	sql = list()
	sql.append("INSERT INTO %s (" % table)
	sql.append(", ".join(key))
	sql.append(") VALUES (")
	sql.append(", ".join(value))
	sql.append(");")
	query = "".join(sql)
	output = dbConnection(database, query)
	if output is None:
		print("[{}] ({}) is/are inserted successfully into Database[{}] and Table[{}]."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), (", ".join(value)), database, table))
		return True
	else:
		print("[{}] ({}) is/are 'NOT' inserted successfully into Database[{}] and Table[{}] and Result Set 'FALSE'."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), (", ".join(value)), database, table))
		return False


def update(database, table, values, condition=None, operator=None):
	""" Update rows into objects table given the key-value pairs in kwargs """
	sql = list()
	sql.append("UPDATE %s SET " % table)
	sql.append(", ".join("%s = '%s'" % (k, v) for k, v in values.items()))
	if condition is not None and operator is not None:
		sql.append(" WHERE")
		sql.append(operator.join(" %s = '%s' " % (k, v) for k, v in condition.items()))
		sql.append(";")
	elif condition is not None and operator is None:
		sql.append(" WHERE")
		sql.append("".join(" %s = '%s' " % (k, v) for k, v in condition.items()))
		sql.append(";")
	else:
		sql.append(";")
	query = "".join(sql)
	db_result = dbConnection(database, query)
	if db_result is None:
		print("[{}] ({}) is/are updated successfully in Database[{}] and Table[{}]."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()),
		              (", ".join("%s = '%s'" % (k, v) for k, v in values.items())), database, table))
		return True
	else:
		print("[{}] ({}) is/are 'NOT' updated successfully in Database[{}] and Table[{}] and Result Set 'FALSE'."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()),
		              (", ".join("%s = '%s'" % (k, v) for k, v in values.items())), database, table))
		return False


def call(database, procedure, parameter=None, output=None):
	""" CALL stored procedure which created in mysql database by given the key-value pairs in kwargs"""
	sql = list()
	sql.append("CALL %s(" % procedure)
	if parameter is not None and output is not None:
		sql.append(", ".join("@%s:='%s'" % (k, v) for k, v in parameter.items()))
		sql.append(", @%s" % output)
		sql.append(");")
	elif parameter is not None and output is None:
		sql.append(", ".join("@%s:='%s'" % (k, v) for k, v in parameter.items()))
		sql.append(");")
	else:
		sql.append(");")
	query = "".join(sql)
	db_result = dbConnection(database, query)
	if db_result is not None:
		print("[{}] Stored Procedure[{}] executed successfully in Database[{}] and output [{}]."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), procedure, database, db_result))
		return db_result
	elif db_result is None:
		print("[{}] Stored Procedure[{}] executed successfully in Database[{}]."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), procedure, database))
		return True
	else:
		print("[{}] Stored Procedure[{}] executed with 'FALSE' Result Set in Database[{}]."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), procedure, database))
		return False


def delete(database, table, condition=None, operator=None):
	""" Delete row in table requires table name, database name, condition, and operator is optional"""
	sql = list()
	sql.append("DELETE FROM %s" % table)
	if condition is not None and operator is not None:
		sql.append(" WHERE ")
		sql.append(operator.join("%s = '%s'" % (k, v) for k, v in condition.items()))
		sql.append(";")
	elif condition is not None and operator is None:
		sql.append(" WHERE ")
		sql.append("".join("%s = '%s'" % (k, v) for k, v in condition.items()))
		sql.append(";")
	else:
		sql.append(";")
	query = "".join(sql)
	db_result = dbConnection(database, query)
	if db_result is None:
		print("[{}] Row(s) deleted successfully in Database[{}] and Table[{}]."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), database, table))
		return True
	else:
		print("[{}] Row(s) 'NOT' deleted successfully in Database[{}] and Table[{}] and Result Set 'FALSE'."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), database, table))
		return False


def truncate(database, table):
	""" Truncate table requires table name and database name"""
	sql = list()
	sql.append("TRUNCATE TABLE %s" % table)
	sql.append(";")
	query = "".join(sql)
	db_result = dbConnection(database, query)
	if db_result is None:
		print("[{}] Table[{}] is truncated successfully in Database[{}]."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), table, database))
		return True
	else:
		print("[{}] Table[{}] is 'NOT' truncated successfully in Database[{}] and Result Set 'FALSE'."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), table, database))
		return False


def custom(database, query):
	""" Execute custom query, have to provide complete query in executable format in QUERY argument """
	db_result = dbConnection(database, query)
	if db_result is None:
		print("[{}] Custom query ({}) is executed successfully with 'NONE' Result Set."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), query))
		return True
	elif db_result is not None:
		print("[{}] Custom query ({}) is executed successfully with 'OUTPUT' Result Set [{}]."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), query, db_result))
		return db_result
	else:
		print("[{}] Custom query ({}) is 'NOT' executed successfully."
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), query))
		return False


def pdf(database, table, df=None, query=None, exec_type='read', index=False, if_exists='replace'):
	try:
		conn = 'mysql+pymysql://root:Ms@240995@localhost:3306/' + database
		engine = create_engine(conn)
		if exec_type == 'read' and query is not None:
			return pd.read_sql_query(sql=query, con=engine)
		elif exec_type == 'read' and query is None:
			return pd.read_sql_table(table_name=table, con=engine)
		elif exec_type == 'write' and df is not None:
			df.to_sql(name=table, con=engine, index=index, if_exists=if_exists)
			print("[{}] DataFrame is inserted successfully into Database[{}] and Table[{}]."
			      .format(time.strftime("%I:%M:%S %p", time.localtime()), database, table))
			return True
		else:
			pass
	except Exception as e:
			print("[{}] Exception Occurs at pipeline.pdf Method. Error: {}"
			      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
	except KeyboardInterrupt as e:
		print("[{}] Someone Forced Program to EXIT - KeyboardInterrupt at pipeline.pdf Method. Error: {}"
		      .format(time.strftime("%I:%M:%S %p", time.localtime()), e))
		exit()

