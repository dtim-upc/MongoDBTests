# MongoExperiments

A set of test cases to evaluate cache performance of MongoDB with different document counts and sizes

# Prerequisites

  - MongoDB
  - PostgreSQL 
  - Working only under linux systems (not windows)
  
# ADBIS 2020 experiments
  - [adbis_experiments](./src/edu/upc/essi/mongo/adbis_experiments/)  contains the experiments for the following use cases
    - E1 - Multivalued attributes
	  - Insert
	  
	  ![insert](ADBIS%202020%20-%20experimental%20results/pngs/e1-insert.png)
	  - Size
	  
	  ![size](ADBIS%202020%20-%20experimental%20results/pngs/e1-size.png)
	  - Sum
	  
	  ![sum](ADBIS%202020%20-%20experimental%20results/pngs/e1-sum.png)
    - E2 - Nested structures
      - Insert
	  
	  ![insert](ADBIS%202020%20-%20experimental%20results/pngs/e2-insert.png)
	  - Size
	  
	  ![size](ADBIS%202020%20-%20experimental%20results/pngs/e2-size.png)
	  - Sum
	  
	  ![sum](ADBIS%202020%20-%20experimental%20results/pngs/e2-sum.png)
    - E3 - Null values     
	  - Insert
	  
	  ![insert](ADBIS%202020%20-%20experimental%20results/pngs/e3-insert.png)
	  - Size
	  
	  ![size](ADBIS%202020%20-%20experimental%20results/pngs/e3-size.png)
	  - Sum
	  
	  ![sum](ADBIS%202020%20-%20experimental%20results/pngs/e3-sum.png)
	  - Count nulls
	  
	  ![count](ADBIS%202020%20-%20experimental%20results/pngs/e3-countnull.png)
    - E4 - Datatype validation
    - E6 - Integrity constraints
	  - Insert
	  
	  ![insert](ADBIS%202020%20-%20experimental%20results/pngs/e4-insert-all.png)
    - E5 - Metadata representation     
	  - Insert: changing number of attributes
	  
	  ![insert](ADBIS%202020%20-%20experimental%20results/pngs/e5-insert.png)
	  - Size: changing number of attributes
	  
	  ![size](ADBIS%202020%20-%20experimental%20results/pngs/e5-size.png)
	  - Sum: changing number of attributes
	  
	  ![sum](ADBIS%202020%20-%20experimental%20results/pngs/e5-sum.png)
	  
	  - Insert: changing data-metadata ratio
	  
	  ![insert](ADBIS%202020%20-%20experimental%20results/pngs/e51-insert.png)
	  - Size: changing data-metadata ratio
	  
	  ![size](ADBIS%202020%20-%20experimental%20results/pngs/e51-size.png)
	  - Sum: changing data-metadata ratio
	  
	  ![sum](ADBIS%202020%20-%20experimental%20results/pngs/e51-sum.png)
  -   [ADBIS 2020 - experimental results](./ADBIS%202020%20-%20experimental%20results) contails the experimental results and the respective figures

# Statistical data for MongoDB inserts
 - Change the constants (Const.java) according to your setup (MongoDB path, logpath, config path, and directories for the database)
 - Run CreateData.java 
      * This will create different databases in different directories and save the ids of the documents in files for the experiments
- Run the code in schema.sql to make the table in PostgreSQL (you might have to create the index before)
- Run RunExper.java with appropriate PostgreSQL settings
  *  This will issue 50 000 queries (10ms sleep inbetween) with random access with primary id on each of the database and record the stats after each 100th query in postgres.
  *  Each database will have 10 runs of 50 000 executions
- After the experiments finish analyse the data with analyse.sql.
  *  This is an example SQL code to retrieve the JSON data from PostgreSQL with pages read, evicted and bytes in cache for both data and index together with the iteration.
  *  you can use the script in excel to get proper graphs.
              
# About the config

I have disabled the compression and limited the cache to 0.25GB to get the cache saturated faster.
