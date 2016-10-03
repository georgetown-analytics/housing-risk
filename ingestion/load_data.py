
#############################################################################






# 
# THIS FILE IS OUTDATED!!
# I merged in the branch to pull in some useful reference code into the postgres.py file, and will soon delete this file.














#############################################################################













#############################################################################
#Initialization
#############################################################################
import logging
import pandas as pd
import datetime
import time

#Configure logging. See /logs/example-logging.py for examples of how to use this.
logging_filename = "../logs/ingestion.log"
logging.basicConfig(filename=logging_filename, level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler()) 			#Pushes everything from the logger to the command line output as well.
logging.warning("--------------------starting load_data module------------------")









#############################################################################
#Classes to hold and manipulate our data
#############################################################################
# Notes:
# 	-After creating this, I found out about Pandas Panels, which are a 3D data model. Maybe should restructure this to use panel instead of a list of snapshots with dataframes.



#Container for a single copy of the sec8contracts database from HUD. Contains both database tables (as pandas dataframes) plus dates.
class Sec8Snapshot(object):
	
	#All properties can be passed on initizalization; but, they can also be set later in the code if needed for clarity (hence the 'None' defaults)
	def __init__(self, contracts_df = None, properties_df = None, date=None, source=''):
		self.contracts_df = contracts_df     	#a Pandas dataframe with the 'contracts' table
		self.properties_df = properties_df		#a Pandas dataframe with the 'properties' table
		self.date = date 						#the date the snapshot was published
		self.source = source					#string representing where the copy came from, e.g. from HUD or from the Internet Archive


#Contains a list of multiple snapshots, as well as methods for comparing the snapshots
class Sec8Timeline(object):

	def __init__(self):
		#A list of Pandas dataframe objects, each representing a copy of the contracts table at a certain time
		self.snapshots = []

	def add(self,snapshot):
		self.snapshots.append(snapshot)
		#TODO do we want to sort the snapshots by their date stamp?

	def remove(self,snapshot):
		self.snapshots.remove(snapshot)








#############################################################################
#Data to be used
#############################################################################
# Data notes:
#	-TODO - should be careful about cluttering global namespace when this is loaded as a module. Should wrap data info in a container object.


#Expected by the load_sec8_contracts function
sec8_flatfile_paths = {
	'current': {'date': datetime.date(2016,8,2),'contracts': "..\data\section8\contracts_database\main-website\sec8contracts_2016-08-02.csv", 'properties': '..\data\section8\contracts_database\main-website\properties_2016-08-02.csv'},
	'2015': {'date': datetime.date(2015,9,5),'contracts': "..\data\section8\contracts_database\internetArchive\sec8contracts_2015-09-05.csv", 'properties': '..\data\section8\contracts_database\internetArchive\properties_2015-09-05.csv'},
	'2011': {'date':datetime.date(2011,9,22),'contracts': "..\data\section8\contracts_database\internetArchive\sec8contracts_2011-09-22.csv", 'properties': '..\data\section8\contracts_database\internetArchive\properties_2011-09-22.csv'},
}







#############################################################################
#Functions
#############################################################################

def load_sec8_contracts(paths):
	"""
	Returns a Sec8Timeline object, which contains a list of Sec8Snapshot objects. Sec8Snapshots are sorted by date (earliest first).
	This function expects a 'paths' variable formatted as a nested dictionary. 
	Each nested entry is the path to two specific CSV files, the 'contracts' and the 'properties' flat files exported from the HUD database.
	Each nested entry is assigned a date, reflecting the data that version of the contracts database was accurate. 
	The Sec8Snapshot objects that are attached to the Timeline each contain two Pandas Dataframes, one for each CSV file.

	paths variable format:
	 {
	 	'snapshot_desc_name': {'date': datetime object, 'contracts':'path\to\contracts.csv', 'properties':'path\to\properties.csv'}
	 }

	CSV files are expected to be in Unicode-8, with the first row as header fields matching the standard HUD format
	"""
	logging.info("Starting 'load_sec8_contracts'. %s snapshots to load; this may take some time... (hint use ctrl+c to interrupt) ",len(paths))
	process_start = time.clock()

	#This is the object we will attach the 
	timeline = Sec8Timeline()
	
	#This loop's purpose is to create each snapshot object and attach it to the timeline
	#The 'paths' variable is a dictionary, so we can't guarantee it will run in a specific order if we just use for key in paths:.
	#Sorted guarantees we add the snapshots to the timeline list in order.
	#key_paths used to disambiguate between the 'key' variable in the sorted function.
	for key_paths in sorted(paths, key=lambda x:(paths[x]['date'])):
		logging.info("  Starting data from '%s'...",key_paths)
		loop_start = time.clock()

		#TODO would be good to add some validation to make sure the format of the CSV files are as we expect.
			#-determine correct encoding (UTF-8)
			#-load the headers of the first file, and make sure headers of all subsequent files match (or, compare to a hard-coded list)

		#Store all the Pandas data frame objects in temporary variables, to be passed to the Snapshot object when it is created.
		contracts_df = pd.read_csv(paths[key_paths]['contracts'], index_col="contract_number",parse_dates=['tracs_effective_date','tracs_overall_expiration_date','tracs_current_expiration_date'])
		logging.info("    Loaded 'contracts'")
		properties_df = pd.read_csv(paths[key_paths]['properties'], index_col="property_id",parse_dates=['ownership_effective_date'])
		logging.info("    Loaded 'properties'")
		date = paths[key_paths]['date']
		snapshot = Sec8Snapshot(contracts_df,properties_df,date)

		timeline.add(snapshot)

		logging.info("  Finished snapshot '%s' from %s. (%s seconds)",key_paths,date,round(time.clock() - loop_start,2))
	
	#End the function
	logging.info("Load finished.\nTotal load time: %s\n------",round(time.clock() - process_start,4))
	logging.info("Total snapshots loaded: %s",len(timeline.snapshots))
	return timeline






#############################################################################
#Other Stuff
#############################################################################
logging.warning("--------------------load_data module concluded------------------")


if __name__ == "__main__":
	sec8_timeline = load_sec8_contracts(sec8_flatfile_paths)