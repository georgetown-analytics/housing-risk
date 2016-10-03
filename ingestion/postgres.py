
import logging
import psycopg2
import datetime
import time
import pandas as pd
import csv


#Configure logging. See /logs/example-logging.py for examples of how to use this.
logging_filename = "../logs/ingestion.log"
logging.basicConfig(filename=logging_filename, level=logging.DEBUG)
logging.warning("--------------------starting module------------------")

#############################
#CONSTANTS
#############################
constants = {
	#used with psycopg2.connect('')
	'db_connect_str': "dbname=temphousingrisk user=postgres password=postgres port=5433",
	'snapshots_csv_filename': 'snapshots_to_load_test.csv',
}

#sample code from http://initd.org/psycopg/docs/usage.html
def sample_add_to_database():
	# Connect to an existing database
	# Troubleshooting notes:
	#  1) Database must already be created
	#  2) Make sure the port matches your copy of the database - this is different per installation. 
	#       Default is 5432, but if you have multiple Postgres installations it may be something else. 
	#       Check your Postgres installation folder \PostgreSQL\9.5\data\postgresql.conf to find your port.
	#  3) user=postgres refers to the default install user, but the password=postgres is set manually during configuration. 
	#     You can either edit your default user password, or add a new user. Note, setting the default user to NULL caused some problems for me.
	#  4) this function will throw an error the second time you run it because it will try to recreate a TABLE that already exists
	conn = psycopg2.connect(constants['db_connect_str'])

	# Open a cursor to perform database operations
	cur = conn.cursor()

	# Execute a command: this creates a new table
	cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")

	# Pass data to fill a query placeholders and let Psycopg perform
	# the correct conversion (no more SQL injections!)
	cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)", (100, "abc'def"))

	# Query the database and obtain data as Python objects
	cur.execute("SELECT * FROM test;")
	print("Retreiving sample data from the database:")
	print(cur.fetchone())

	# Make the changes to the database persistent
	conn.commit()

	# Close communication with the database
	cur.close()
	conn.close()



def quick_add_contracts_tables():

	# Get the list of files to load - using Pandas dataframe (df), although we don't need most of the functionality that Pandas provides.
	paths_df = pd.read_csv(constants['snapshots_csv_filename'], parse_dates=['date'])

	#Example of how to access the filenames we will need to use
	print(paths_df.get_value(0,'contracts_csv_filename'))
	
	# Connect to the database
	conn = psycopg2.connect(constants['db_connect_str'])
	cur = conn.cursor()

	for index, row in paths_df.iterrows():
		
		#Get the full paths to the CSV files
		contracts_path = row['folder_path'] + row['contracts_csv_filename']
		properties_path = row['folder_path'] + row['properties_csv_filename']

		file = open(contracts_path, 'r')	#'r' means open the file for reading only

		# Identify which columns to read/write from CSV to SQL
		# Two different formats, one for CSV reading and one for SQL writing.
		# This text can be created using  join on the rows of the contracts_columns.csv file.
		#	     Columns related to rents for each bedroom type start with integers, which are not valid names for columns in SQL.
		#	     Current code renames these columns by appending a 'd' to teh start of the column name. 
		#		 Therefore, we need to use column order instead of column names when reading/writing
		#        
		columns_list = ('contract_number','property_id','property_name_text','tracs_effective_date','tracs_overall_expiration_date','tracs_overall_exp_fiscal_year','tracs_overall_expire_quarter','tracs_current_expiration_date','tracs_status_name','contract_term_months_qty','assisted_units_count','is_hud_administered_ind','is_acc_old_ind','is_acc_performance_based_ind','contract_doc_type_code','program_type_name','program_type_group_code','program_type_group_name','rent_to_FMR_ratio','rent_to_FMR_description', 'd0BR_count','d1BR_count','d2BR_count','d3BR_count','d4BR_count','d5plusBR_count','d0BR_FMR','d1BR_FMR','d2BR_FMR','d3BR_FMR','d4BR_FMR')
		columns_SQL_query = "id serial PRIMARY KEY, contract_number varchar(255), property_id integer, property_name_text varchar(255), tracs_effective_date DATE, tracs_overall_expiration_date DATE, tracs_overall_exp_fiscal_year integer, tracs_overall_expire_quarter varchar(2), tracs_current_expiration_date DATE, tracs_status_name varchar(255), contract_term_months_qty integer, assisted_units_count integer, is_hud_administered_ind varchar(1), is_acc_old_ind varchar(1), is_acc_performance_based_ind varchar(1), contract_doc_type_code varchar(64), program_type_name varchar(255), program_type_group_code varchar(64), program_type_group_name varchar(255), rent_to_FMR_ratio Decimal(6,2), rent_to_FMR_description varchar(255), d0BR_count integer, d1BR_count integer, d2BR_count integer, d3BR_count integer, d4BR_count integer, d5plusBR_count integer, d0BR_FMR Decimal(19,4), d1BR_FMR Decimal(19,4), d2BR_FMR Decimal(19,4), d3BR_FMR Decimal(19,4), d4BR_FMR Decimal(19,4)"
			
			
		#Create the needed database table, removing it if it already exists
		tablename="con_" + row['ref_name']
		cur.execute("DROP TABLE " + tablename + ";")
		cur.execute("CREATE TABLE " + tablename + " (" + columns_SQL_query + ");")

		# Get data from CSV into SQL. TODO - this for loop is definitely going to be slower than desired.
		#csv_reader = csv.DictReader(file,dialect='excel', fieldnames=columns_list)
		#for row in csv_reader:
		#	cur.execute("INSERT INTO " + tablename + "VALUES")
		
		#This method only works if the full table is added. Using the for row in csv_reader method instead
		cur.copy_from(file, tablename, sep=',', columns=columns_list)
		file.close()


	#Make changes persistent and close the database
	conn.commit()
	cur.close()
	conn.close()

if __name__ == '__main__':
	#sample_add_to_database()
	quick_add_contracts_tables()




