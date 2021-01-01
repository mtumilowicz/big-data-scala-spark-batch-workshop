[![Build Status](https://travis-ci.com/mtumilowicz/big-data-scala-spark-batch-workshop.svg?branch=master)](https://travis-ci.com/mtumilowicz/big-data-scala-spark-batch-workshop)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

# big-data-scala-spark-batch-workshop
* references
    * [Learning Spark, 2nd Edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
    * [Spark in Action, Second Edition](https://www.manning.com/books/spark-in-action-second-edition)
    * https://medium.com/@mrpowers/testing-spark-applications-8c590d3215fa
    * https://stackoverflow.com/questions/43729262/how-to-write-unit-tests-in-spark-2-0/50581218#50581218
    * https://sparkbyexamples.com/spark/spark-read-and-write-json-file/
    * https://sparkbyexamples.com/spark/spark-schema-explained-with-examples/
    * https://sparkbyexamples.com/spark/spark-read-and-write-json-file/
    * https://bigdataprogrammers.com/merging-two-dataframes-in-spark/
    * https://mungingdata.com/apache-spark/aggregations/
    * https://spark.apache.org/docs/3.0.0-preview/sql-getting-started.html#running-sql-queries-programmatically
    * https://stackoverflow.com/a/43812193 (for windows)
    * https://sparkbyexamples.com/spark/spark-sql-dataframe-join/
    * https://towardsdatascience.com/write-clean-and-solid-scala-spark-jobs-28ac4395424a
    * https://www.edureka.co/blog/spark-architecture/
    * https://spark.apache.org/docs/latest/cluster-overview.html
    * https://queirozf.com/entries/apache-spark-architecture-overview-clusters-jobs-stages-tasks
    * https://data-flair.training/blogs/apache-spark-rdd-vs-dataframe-vs-dataset/
    * https://medium.com/@venkat34.k/the-three-apache-spark-apis-rdds-vs-dataframes-and-datasets-4caf10e152d8
    * https://towardsdatascience.com/strategies-of-spark-join-c0e7b4572bcf
    * https://medium.com/datakaresolutions/optimize-spark-sql-joins-c81b4e3ed7da
    * https://databricks.com/glossary/tungsten
    * https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-tungsten.html

## preface

## spark
* a unified engine designed for large-scale distributed data processing, on premises in data centers 
  or in the cloud
* provides in-memory storage for intermediate computations
    * faster than Hadoop MapReduce
* incorporates libraries for
    * machine learning (MLlib)
    * SQL for interactive queries (Spark SQL)
    * stream processing (Structured Streaming) for interacting with real-time data
    * graph processing (GraphX)
* four steps of a typical Spark scenario
    1 Ingestion
        * at this stage, the data is raw
    1 Improvement of data quality (DQ)
        * example: ensure that all birth dates are in the past
        * example: obfuscate Social Security numbers (SSNs)
    1 Transformation
        * example: join with other datasets, perform aggregations
    1 Publication
        * people in your organization can perform actions on it and make decisions based on it
        * example: load the data into a data warehouse, save in a file on S3
# components overview
* components and architecture
    ![alt text](img/architecture.png)
    * SparkSession
        * provides a single unified entry point to all of Spark’s functionality
            * defining DataFrames and Datasets
            * reading from data sources
            * writing to data lakes
            * accessing catalog metadata
            * issuing Spark SQL queries
        * there is a unique SparkSession for your application, whether you are in local mode or have 10,000 nodes
    * Spark driver
        * process running the `main()` function of the application
        * instantiates SparkSession
        * communicates with the cluster manager
        * requests resources (CPU, memory, etc.) from the cluster manager for Spark’s executors (JVMs)
        * transforms operations into DAG computations and schedules them
            * directed acyclic graph (DAG) is a finite directed graph with no directed cycles
        * distributes operations execution as tasks across the Spark executors
            * does not run computations (filter,map, reduce, etc)
        * once the resources are allocated, it communicates directly with the executors
        * resides on master node
    * Cluster manager
        * manages and allocates resources for the cluster of nodes on which your Spark application runs
        * supports four cluster managers
            * the built-in standalone cluster manager
            * Apache Hadoop YARN
            * Apache Mesos
            * Kubernetes
    * Worker node
        * any node that can run application code in the cluster
        * may not share filesystems with one another
    * Spark executor
        * runs on each worker node in the cluster
            * only a single executor runs per node
        * communicates with the driver program
        * executes tasks on the workers
    * Job
        * parallel computation consisting of multiple tasks that gets spawned in response
          to a Spark action (e.g. save, collect)
        * is a sequence of Stages, triggered by an action (ex. `.count()`)
    * Stage
        * a sequence of Tasks that can all be run together, in parallel, without a shuffle
        * example: using `.read.map.filter` can all be done without a shuffle, so it can fit in a single stage
    * Task
        * unit of work that will be sent to one executor
        * is a single operation (ex. `.map` or `.filter`) applied to a single Partition
        * each Task is executed as a single thread in an Executor
        * example: if your dataset has 2 Partitions, an operation such as a `filter()` will trigger 2 Tasks,
          one for each Partition
    * Shuffle
        * operation where data is re-partitioned across a Cluster
        * costly operation because a lot of data can be sent via the network
        * example: join and any operation that ends with ByKey will trigger a Shuffle
    * Partition
        * data is split into Partitions so that each Executor can operate on a single part, enabling parallelization
        * example: ingesting the CSV file in a distributed way
            * file must be on a shared drive, distributed filesystem (like HDFS), or shared
              filesystem mechanism such as Dropbox
            * workers will create tasks to read the file
                * worker will assign a memory partition to the task
                * task will read a part of the CSV file and stores them in a dedicated partition
        * why should you care?
            * joining data from the first partition of worker 1 with data in the second partition of worker 2
                * all that data will have to be transferred, which is a costly operation
            * solution: repartition the data
## data representation
* RDD (Resilient Distributed Datasets)
    * fundamental data structure of Spark
    * immutable distributed collection of data
        * data itself is in partitions
* Dataset
    * take on two characteristics: typed and untyped APIs
    * think of a DataFrame in Scala as an alias for a collection of generic objects, `Dataset[Row]`
        * Row is a generic untyped JVM object that may hold different types of fields
            * uses efficient storage called Tungsten
        * DataFrames are like distributed in-memory tables with named columns and
          schemas, where each column has a specific data type: integer, string, array, map, etc
            * there are no primary or foreign keys or indexes in Spark
            * data can be nested, as in a JSON or XML document
                * however to perform an analytical operation it's often useful to flattening JSON 
                  structure (transforming its hierarchical data elements into tabular formats)
                    * example: perform aggregates (group by) or joins
                    * flattening JSON = converting the structures into fields and exploding the 
                      arrays into distinct rows
                        * `.withColumn("items", explode(df.col("books")))`
        * get first column of given row: `val name = row.getString(0)`
    * Dataset is a collection of strongly typed JVM objects
        * has also an untyped view called a DataFrame, which is a Dataset of Row
    * Converting DataFrames to Datasets
        ```
        val bloggersDS = spark
          .read
          .json("path")
          .load()
          .as[TargetClass]
        ```
* schemas
    * defines the column names and associated data types for a DataFrame
    * defining vs inferring a schema
        * no inferring data types
        * no separate job just to read a large portion of file to ascertain the schema
            * for a large data file can be expensive and time-consuming
        * errors detection if data doesn’t match the schema
    * ways to define a schema
        * programmatically: `val schema = StructType(Array(StructField("author", StringType, false)`
        * DDL: `val schema = "author STRING, title STRING, pages INT"`

## data import / export
* typical use case is to ingest data from an on-premises database, and write the data into 
  cloud storage (for example, Amazon S3)
* A data source could be any of the following:
    * A file (CSV, JSON, XML, Avro, Parquet, and ORC, etc)
    * A relational and nonrelational database
    * other data provider: (REST) service, etc
* DataFrameReader
    * core construct for reading data into a DataFrame from myriad
      data sources in formats such as JSON, CSV, Parquet, Text, Avro, ORC, etc.
    * can only be accessed through a SparkSession instance
* DataFrameWriter
    * it saves or writes data to a specified built-in data source
    * after the file(s) have been successfully exported, Spark will add a _SUCCESS file to the
      directory
    
### file formats
* problem with traditional file formats
    * JSON and XML are not easy to split and big data files need to be splittable
    * CSV cannot store hierarchical information as JSON or XML can
    * none are designed to incorporate metadata.
    * formats are quite verbose (especially JSON and XML), which inflates the file size drastically
* big data brings its own set of file formats: Avro, ORC, or Parquet   
    * Avro 
        * schema-based serialization format (binary data)
        * supports dynamic modification of the schema
        * is row-based, so easier to split
    * ORC 
        * columnar storage format
        * supports compression
    * Parquet 
        * columnar storage format
        * supports compression
        * add columns at the end of the schema
        * Parquet metadata usually contains the schema
            * if the DataFrame is written as Parquet, the schema is preserved as part of the Parquet metadata
              * subsequent reads do not require you supply a schema
        * default and preferred data source for Spark
        * files are stored in a directory structure that contains the data files, metadata,
          a number of compressed files, and some status files
## data transformation
* operations can be classified into two types
    * transformations
        * `DataFrame -> DataFrame`
        * example: `select()`, `filter()`
        * evaluated lazily
            * action triggers evaluation
            * results are not computed immediately, but they are recorded or remembered as a lineage
                * allows Spark to optimize queries (rearrange certain transformations, coalesce them, etc.)
                * provides fault tolerance: can reproduce its original state by simply replaying the 
                  recorded lineage
        * two types
            * narrow
                * single output partition is computed from a single input partition
                * example: `filter()`, `contains()`
            * wide
                * data from other partitions is read in, combined, and written to disk
                * example: `groupBy()`, `orderBy()`
    * actions
        * example: `count()`, `save()`
    
### aggregations
* DataFrame API
    ```
    Dataset<Row> apiDf = df
        .groupBy(col("firstName"), col("lastName"), col("state"))
        .agg(
            sum("quantity"),
            sum("revenue"),
            avg("revenue"));
    ```
* SQL
    ```
    df.createOrReplaceTempView("orders");
  
    String sqlStatement = "SELECT " +
        " firstName, " +
        " lastName, " +
        " state, " +
        " SUM(quantity), " +
        " SUM(revenue), " +
        " AVG(revenue) " +
        " FROM orders " +
        " GROUP BY firstName, lastName, state";
  
    Dataset<Row> sqlDf = spark.sql(sqlStatement);
    ```
### joins
* Broadcast Hash Join (map-side-only join)
    * used when joining one small and large
        * small = fitting in the driver’s and executor’s memory
    * smaller data set is broadcasted by the driver to all Spark executors
    * by default if the smaller data set is less than 10 MB
    * does not involve any shuffle of the data set
* Shuffle Sort Merge Join
    * used when joining two large data sets
    * default join algorithm
    * pre-requirement: partitions have to be co-located
        * all rows having the same value for the join key should be stored in the same partition 
        * otherwise, there will be shuffle operations to co-locate the data
    * has two phases
        * sort phase
            * sorts each data set by its desired join key
        * merge phase
            * iterates over each key in the row from each data set and merges 
              the rows if the two keys match
### sql
* tables
    ```
    ids.write.
      option("path", "/tmp/five_ids").
      saveAsTable("five_ids")
    ```
    * each table is associated with its relevant metadata (the schema, partitions, physical location
      where the actual data resides, etc.)
        * all metadata is stored in a central metastore
            * by default: Apache Hive metastore
                * Catalog is the interface for managing a metastore
                    * spark.catalog.listDatabases()
                    * spark.catalog.listTables()
                    * spark.catalog.listColumns("us_delay_flights_tbl")
                * location: /user/hive/warehouse
    * two types of tables
        * managed
            * Spark manages metadata and the data
            * example: local filesystem, HDFS, Amazon S3
            * note that SQL command such as DROP TABLE deletes both the metadata and the data
                * with an unmanaged table, the same command will delete only the metadata
        * unmanaged
            * Spark only manages the metadata
            * example: Cassandra
    * reside within a database
* views
    * vs table: views don’t actually hold the data
        * tables persist after application terminates, but views disappear
    * to enable a table-like SQL usage in Spark - create a view
        ```      
        df.createOrReplaceTempView("geodata");
      
        Dataset<Row> smallCountries = spark.sql("SELECT * FROM ...");
        ```
              
## deployment
* Mode: Local
    * Spark driver: Runs on a single JVM, like a laptop or single node
    * Spark executor: Runs on the same JVM as the driver
    * Cluster manager: Runs on the same host
* Mode: Kubernetes
    * Spark driver: Runs in a Kubernetes pod
    * Spark executor: Each worker runs within its own pod
    * Cluster manager: Kubernetes Master
    
## optimizations
* at the core of the Spark SQL engine are the Catalyst optimizer and Project Tungsten.
### tungsten
* focuses on enhancing three key areas
    * memory management and binary processing
        * manage memory explicitly
            * off-heap binary memory management
        * eliminate the overhead of JVM object model and garbage collection
            * Java objects have large overheads — header info, hashcode, Unicode info, etc.
            * instead use binary in-memory data representation aka Tungsten row format
    * cache-aware computation
        * algorithms and data structures to exploit memory hierarchy (L1, L2, L3)
        * cache-aware computations with cache-aware layout for high cache hit rates
    * code generation
        * exploit modern compilers and CPUs
        * generates JVM bytecode to access Tungsten-managed memory structures that gives a very fast access
        * uses the Janino compiler - super-small, super-fast Java compiler
### catalyst
* like an RDBMS query optimizer
* converts computational query and converts it into an execution plan
    ![alt text](img/optimization.jpg)
* Phase 1: Analysis
    * Spark SQL engine generates AST tree for the SQL or DataFrame query
* Phase 2: Logical optimization
    * Catalyst optimizer will construct a set of multiple plans and then, using its cost-based
      optimizer (CBO), assign costs to each plan
* Phase 3: Physical planning
    * Spark SQL generates an optimal physical plan for the selected logical plan
* Phase 4: Code generation
    * generating efficient Java bytecode to run on each machine
### caching
* if you reuse a dataframe for different analyses, it is a good idea to cache it
    * steps are executed each time you run an analytics pipeline
    * example: DataFrames commonly used during iterative machine learning training
* caching vs persistence
    * persist() provides more control over how and where data is stored
        * DISK_ONLY, OFF_HEAP, etc.
    * when you use cache() or persist(), the DataFrame is not fully cached until you invoke
      an action that goes through every record
        * partitions cannot be fractionally cached (e.g., if you have 8 partitions but only 4.5
          partitions can fit in memory, only 4 will be cached)
* vs checkpointing
    * checkpoint() method will truncate the DAG (or logical plan) and save the content of the
      dataframe to disk
    * cache will be cleaned when the session ends
        * checkpoints will stay on disk
* note that a lot of the issues can come from key skewing: the data is so fragmented among
  partitions that a join operation becomes very long
    * solution: coalesce(), repartition(), or repartitionByRange()
### user-defined functions
```
val cubed = (s: Long) => { s * s * s } // define function

spark.udf.register("cubed", cubed) // register UDF

spark.sql("SELECT id, cubed(id) AS id_cubed FROM ...") // use
```
* excellent choice for performing data quality rules
* UDF’s internals are not visible to Catalyst
    * UDF is treated as a black box for the optimizer
    * Spark won’t be able to analyze the context where the UDF is called
        * if you make dataframe API calls before or after, Catalyst can’t optimize
          the full transformation
        * should be at the beginning or the end of transformations