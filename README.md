# MongoExperiments

A set of test cases to evaluate cache performance of MongoDB with different document counts and sizes

# Prerequisites

  - MongoDB
  - PostgreSQL 
  - Working only under linux systems (not windows)
  ![Screenshot](ADBIS%202020%20-%20experimental%20results/pngs/e1-insert.png)
# IDEAS 2020 experiments
  - [adbis_experiments](./src/edu/upc/essi/mongo/adbis_experiments/)  contains the experiments for the following use cases
    - E1 - Multivalued attributes
 <p align="center">Insert</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e1-insert.png" alt="E1-insert" title="Insert"/></p>
 <p align="center">Size</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e1-size.png" alt="E1-size" title="Size"/></p>
 <p align="center">Sum</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e1-sum.png" alt="E1-query" title="Sum"/></p>
    - E2 - Nested structures
     <p align="center">Insert</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e2-insert.png" alt="E2-insert" title="Insert"/></p>
 <p align="center">Size</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e2-size.png" alt="E2-size" title="Size"/></p>
 <p align="center">Sum</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e2-sum.png" alt="E2-query" title="Sum"/></p>
    - E3 - Null values     <p align="center">Insert</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e3-insert.png" alt="E3-insert" title="Insert"/></p>
 <p align="center">Size</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e3-size.png" alt="E3-size" title="Size"/></p>
 <p align="center">Sum</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e3-sum.png" alt="E3-query" title="Sum"/></p>
<p align="center">Count</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e3-countnull.png" alt="E3-query" title="Count"/></p>
- E4 - Datatype validation
    - E6 - Integrity constraints
    <p align="center">Insert</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e4-insert-all.png" alt="E4-insert" title="Insert "/></p>
    - E5 - Metadata representation
    <p align="center">Insert: changing number of attributes</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e5-insert.png" alt="E5-insert" title="Insert:changing number of attributes"/></p>
 <p align="center">Size: changing number of attributes</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e5-size.png" alt="E5-size" title="Size: changing number of attributes"/></p>
 <p align="center">Sum: changing number of attributes</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e5-sum.png" alt="E1-query" title="Sum: changing number of attributes"/></p>
    <p align="Insert: changing data-metadata ratio">Insert</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e51-insert.png" alt="E1-insert" title="Insert: changing data-metadata ratio"/></p>
 <p align="center">Size: changing data-metadata ratio</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e51-size.png" alt="E51-size" title="Size: changing data-metadata ratio"/></p>
 <p align="center">Sum: changing data-metadata ratio</p>
<p align="center"><img src="./ADBIS 2020 - experimental results/pngs/e51-sum.png" alt="E51-query" title="Sum: changing data-metadata ratio"/></p>
  -   [ADBIS 2020 - experimental results](./ADBIS 2020 - experimental results) contails the experimental results and the respective figures

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
