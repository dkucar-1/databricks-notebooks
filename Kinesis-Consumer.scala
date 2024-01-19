// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png" style="float: left: margin: 20px"/>
// MAGIC
// MAGIC # Streaming Live Data using Apache Spark with Databricks, S3 and Kinesis
// MAGIC
// MAGIC We have another server that reads Wikipedia edits in real time, with a multitude of different languages. 
// MAGIC
// MAGIC **What you will learn:**
// MAGIC * About Kinesis Streams
// MAGIC * How to establish a connection with Kinesis in Spark
// MAGIC * How to use Structured Streaming in Spark
// MAGIC * Visualizations
// MAGIC
// MAGIC ## Technical Requirements
// MAGIC
// MAGIC * AWS account
// MAGIC * Kinesis Streams
// MAGIC * Web browser: current versions of Google Chrome, Firefox, Safari, Microsoft Edge and 
// MAGIC Internet Explorer 11 on Windows 7, 8, or 10 (see <a href="https://docs.databricks.com/user-guide/supported-browsers.html#supported-browsers#" target="_blank">Supported Web Browsers</a>)
// MAGIC * Latest Databricks Runtime
// MAGIC
// MAGIC :NOTE: AWS charges for Kinesis provisioning AND throughput. So, even if your Kinesis stream is inactive, you pay for provisioning 
// MAGIC (<a href="https://aws.amazon.com/kinesis/data-streams/pricing/" target="_blank">Kinesis Pricing</a>)
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Kinesis Streams </h2>
// MAGIC
// MAGIC
// MAGIC Kinesis can be used in a variety of applications such as
// MAGIC * Analytics pipelines, such as clickstreams
// MAGIC * Anomaly detection (fraud/outliers)
// MAGIC * Application logging
// MAGIC * Archiving data
// MAGIC * Data collection from IoT devices
// MAGIC * Device telemetry streaming
// MAGIC * Transaction processing
// MAGIC * User telemetry processing
// MAGIC * <b>Live dashboarding</b>
// MAGIC
// MAGIC In this notebook, we will show you how to use Kinesis to produce LIVE Dashboards.

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Configure Authentication</h2>
// MAGIC
// MAGIC First, we pass these variables in as options i.e. option("key", "value")
// MAGIC * `accessKey`: your Amazon access key (be very careful with)
// MAGIC * `secretKey`: your Amazon secret key (be very careful with)
// MAGIC * <b>you should probably set those using IAM roles in a production environment</b>

// COMMAND ----------

val accessKey = // ENTER YOUR AWS ACCESS KEY
val secretKey = // ENTER YOUR AWS SECRET KEY

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Kinesis Configuration</h2>
// MAGIC
// MAGIC We will also need to define these variables and pass them in as options i.e. `option("key", "value")`
// MAGIC * `kinesisRegion`: closest AWS region i.e. `us-west-2` (Oregon)
// MAGIC * `maxRecordsPerFetch`: maximum records per fetch
// MAGIC * `kinesisStreamName`: the name of your stream exactly as on the AWS Kinesis portal (image below)
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/Structured-Streaming/kinesis-streams-dashboard.png"/>

// COMMAND ----------

val kinesisStreamName = // Enter your Kinesis Stream Name (like in Kinesis portal)   
val kinesisRegion = "us-west-2"  // Change this to your AWS region, if applicable
val maxRecordsPerFetch = 10                              // Throttle to 10 records per batch (slow)

// COMMAND ----------

// MAGIC %md 
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> The Kinesis Schema</h2>
// MAGIC
// MAGIC Reading from Kinesis returns a `DataFrame` with the following fields:
// MAGIC
// MAGIC | Field             | Type   | Description |
// MAGIC |------------------ | ------ |------------ |
// MAGIC | **partitionKey**  | string | A unique identifier to a partition which is used to group data by shard within a stream |
// MAGIC | **data**          | binary | Our JSON payload. We'll need to cast it to STRING |
// MAGIC | **stream**     | string | A sequence of data records |
// MAGIC | **shardID**        | string | An identifier to a uniquely identified sequence of data records in a stream|
// MAGIC | **sequenceNumber**     | long   | A unique identifier for a packet of data (alternative to a timestamp) |
// MAGIC | **approximateArrivalTimestamp** 	| timestamp | Time when data arrives |
// MAGIC
// MAGIC
// MAGIC Kinesis terminology is explained <a href="https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html" target="_blank">HERE</a>.
// MAGIC
// MAGIC In the example below, the only column we want to keep is `data`.

// COMMAND ----------

// MAGIC %md We should probably run the producer code at this point.

// COMMAND ----------

// MAGIC %run "./Kinesis-Producer"

// COMMAND ----------

// MAGIC %scala
// MAGIC spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
// MAGIC
// MAGIC val editsDF = spark.readStream                      // Get the DataStreamReader
// MAGIC   .format("kinesis")                                // Specify the source format as "kinesis"
// MAGIC   .option("streamName", kinesisStreamName)          // Stream name exactly as in AWS Kinesis portal
// MAGIC   .option("region", kinesisRegion)                  // AWS region like us-west-2 (Oregon)
// MAGIC   .option("maxRecordsPerFetch", maxRecordsPerFetch) // Maximum records per fetch
// MAGIC   .option("initialPosition", "EARLIEST")            // Specify starting position of stream that hasn't expired yet (up to 7 days)
// MAGIC   .option("awsAccessKey", accessKey)                // AWS Access Key 
// MAGIC   .option("awsSecretKey", secretKey)                // AWS Secret Key
// MAGIC   .load()                                           // Load the DataFrame
// MAGIC   .selectExpr("CAST(data as STRING) as body")       // Cast the "data" column to STRING, rename to "body"

// COMMAND ----------

// MAGIC %md Let's display some data.
// MAGIC
// MAGIC :CAUTION: Make sure the Wikipedia stream server is running at this point.
// MAGIC
// MAGIC How long does Kinesis hold streaming data? Can I buffer it a bit?
// MAGIC

// COMMAND ----------

// MAGIC %scala
// MAGIC val myStream = "my_scala_stream"
// MAGIC display(editsDF,  streamName = myStream)

// COMMAND ----------

// MAGIC %md
// MAGIC Make sure to stop the stream before continuing.

// COMMAND ----------

// MAGIC %scala
// MAGIC for (s <- spark.streams.active) { // Iterate over all active streams
// MAGIC   if (s.name == myStream) {       // Look for our specific stream
// MAGIC     println("Stopping "+s.name)   // A little extra feedback
// MAGIC     s.stop                        // Stop the stream
// MAGIC   }
// MAGIC }

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Use Kinesis to Display the Raw Data</h2>
// MAGIC
// MAGIC The Kinesis server acts as a sort of asynchronous buffer and displays raw data.
// MAGIC
// MAGIC Please use the Kinesis Stream Server notebook to add content to the stream. 
// MAGIC
// MAGIC Since raw data coming in from a stream is transient, we'd like to save it to a more permanent data structure.
// MAGIC
// MAGIC The first step is to define the schema for the JSON payload.

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType}
// MAGIC
// MAGIC lazy val schema = StructType(List(
// MAGIC   StructField("bot", BooleanType, true),
// MAGIC   StructField("comment", StringType, true),
// MAGIC   StructField("id", IntegerType, true),                  // ID of the recentchange event 
// MAGIC   StructField("length",  StructType(List( 
// MAGIC     StructField("new", IntegerType, true),               // Length of new change
// MAGIC     StructField("old", IntegerType, true)                // Length of old change
// MAGIC   )), true), 
// MAGIC   StructField("meta", StructType(List(  
// MAGIC 	StructField("domain", StringType, true),
// MAGIC 	StructField("dt", StringType, true),
// MAGIC 	StructField("id", StringType, true),
// MAGIC 	StructField("request_id", StringType, true),
// MAGIC 	StructField("schema_uri", StringType, true),
// MAGIC 	StructField("topic", StringType, true),
// MAGIC 	StructField("uri", StringType, true),
// MAGIC 	StructField("partition", StringType, true),
// MAGIC 	StructField("offset", StringType, true)
// MAGIC   )), true),
// MAGIC   StructField("minor", BooleanType, true),                 // Is it a minor revision?
// MAGIC   StructField("namespace", IntegerType, true),             // ID of relevant namespace of affected page
// MAGIC   StructField("parsedcomment", StringType, true),          // The comment parsed into simple HTML
// MAGIC   StructField("revision", StructType(List(                 
// MAGIC     StructField("new", IntegerType, true),                 // New revision ID
// MAGIC     StructField("old", IntegerType, true)                  // Old revision ID
// MAGIC   )), true),
// MAGIC   StructField("server_name", StringType, true),
// MAGIC   StructField("server_script_path", StringType, true),
// MAGIC   StructField("server_url", StringType, true),
// MAGIC   StructField("timestamp", IntegerType, true),             // Unix timestamp 
// MAGIC   StructField("title", StringType, true),                  // Full page name
// MAGIC   StructField("type", StringType, true),                   // Type of recentchange event (rc_type). One of "edit", "new", "log", "categorize", or "external".
// MAGIC   StructField("geolocation", StructType(List(              // Geo location info structure
// MAGIC     StructField("PostalCode", StringType, true),
// MAGIC     StructField("StateProvince", StringType, true),
// MAGIC     StructField("city", StringType, true), 
// MAGIC     StructField("country", StringType, true),
// MAGIC     StructField("countrycode3", StringType, true)          // Really, we only need the three-letter country code in this exercise
// MAGIC    )), true),
// MAGIC   StructField("user", StringType, true),                   // User ID of person who wrote article
// MAGIC   StructField("wiki", StringType, true)                    // wfWikiID
// MAGIC ))

// COMMAND ----------

// MAGIC %md 
// MAGIC Next we can use the function `from_json` to parse out the full message with the schema specified above.

// COMMAND ----------

// MAGIC %scala
// MAGIC import org.apache.spark.sql.functions.from_json
// MAGIC
// MAGIC val jsonEdits = editsDF.select(
// MAGIC   from_json($"body", schema).as("json"))   // Parse the column "value" and name it "json"

// COMMAND ----------

// MAGIC %md 
// MAGIC
// MAGIC When parsing a value from JSON, we end up with a single column containing a complex object.
// MAGIC
// MAGIC We can clearly see this by simply printing the schema.

// COMMAND ----------

// MAGIC %scala
// MAGIC jsonEdits.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC The fields of a complex object can be referenced with a "dot" notation as in:
// MAGIC
// MAGIC `$"json.wiki"`
// MAGIC
// MAGIC A large number of these fields/columns can become unwieldy.
// MAGIC
// MAGIC For that reason, it is common to extract the sub-fields and represent them as first-level columns as seen below:

// COMMAND ----------

// MAGIC %scala
// MAGIC val wikiDF = jsonEdits
// MAGIC   .select($"json.wiki".as("wikipedia"),                         // Promoting from sub-field to column
// MAGIC           $"json.namespace".as("namespace"),                    //     "       "      "      "    "
// MAGIC           $"json.title".as("page"),                             //     "       "      "      "    "
// MAGIC           $"json.server_name".as("pageURL"),                    //     "       "      "      "    "
// MAGIC           $"json.user".as("user"),                              //     "       "      "      "    "
// MAGIC           $"json.geolocation.countrycode3".as("countryCode3"),  //     "       "      "      "    "
// MAGIC           $"json.timestamp".cast("timestamp"))                  // Promoting and converting to a timestamp
// MAGIC   .filter($"wikipedia".isNotNull)

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Mapping Anonymous Editors' Locations</h2>
// MAGIC
// MAGIC When you run the query, the default is a [live] html table.
// MAGIC
// MAGIC The geocoded information allows us to associate an anonymous edit with a country.
// MAGIC
// MAGIC We can then use that geocoded information to plot edits on a [live] world map.
// MAGIC
// MAGIC In order to create a slick world map visualization of the data, you'll need to click on the item below.
// MAGIC
// MAGIC Under <b>Plot Options</b>, use the following:
// MAGIC * <b>Keys:</b> `countryCode3`
// MAGIC * <b>Values:</b> `count`
// MAGIC
// MAGIC In <b>Display type</b>, use <b>World map</b> and click <b>Apply</b>.
// MAGIC
// MAGIC <img src="https://files.training.databricks.com/images/eLearning/Structured-Streaming/plot-options-map-04.png"/>
// MAGIC
// MAGIC By invoking a `display` action on a DataFrame created from a `readStream` transformation, we can generate a LIVE visualization!
// MAGIC
// MAGIC :CAUTION: Make sure the Wikipedia stream server is running at this point.
// MAGIC
// MAGIC :SIDENOTE: Keep an eye on the plot for a minute or two and watch the colors change.

// COMMAND ----------

// MAGIC %scala
// MAGIC val mappedDF = wikiDF
// MAGIC   .groupBy("countryCode3")   // Aggregate by country (code)
// MAGIC   .count()                   // Produce a count of each aggregate
// MAGIC
// MAGIC display(mappedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Write Some Data to S3 Using Databricks Delta</h2>
// MAGIC
// MAGIC Let's say we want to write some valuable aggregation data to S3.
// MAGIC
// MAGIC Wait a bit here so some data can be written.

// COMMAND ----------

val myPath = "/tmp/delta"

wikiDF.groupBy("countryCode3")   // Aggregate by country (code)
  .count()                       // Produce a count of each aggregate
  .writeStream
  .format("delta") 
  .option("checkpointLocation", myPath + "/_checkpoint") 
  .outputMode("complete") 
  .start(myPath)

// COMMAND ----------

// MAGIC %md After a bit..

// COMMAND ----------

// MAGIC %fs ls /tmp/delta

// COMMAND ----------

// MAGIC %md
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Cleanup </h2>

// COMMAND ----------

// MAGIC %md Stop the streams.

// COMMAND ----------

// MAGIC %scala
// MAGIC for (s <- spark.streams.active)  // Iterate over all active streams
// MAGIC   s.stop()          

// COMMAND ----------

// MAGIC %md Remove S3 write directory.

// COMMAND ----------

dbutils.fs.rm("/tmp/delta", true)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC <h2><img src="https://files.training.databricks.com/images/105/logo_spark_tiny.png"> Additional Topics &amp; Resources</h2>
// MAGIC
// MAGIC * <a href="https://www.tutorialspoint.com/amazon_web_services/amazon_web_services_kinesis.htm" target="_blank">Introduction to Kinesis</a>
// MAGIC * <a href="https://docs.aws.amazon.com/streams/latest/dev/introduction.html" target="_blank">AWS Kinesis Streams Developer Guide</a>
// MAGIC * <a href="https://docs.databricks.com/spark/latest/structured-streaming/kinesis.html" target="_blank">Databricks Integration with Kinesis Guide</a>
// MAGIC * <a href="https://pypi.org/project/sseclient/" target="_blank">Python SSE Client</a>
// MAGIC * <a href="https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kinesis.html" target="_blank">Python Boto3 library for Kinesis</a>
