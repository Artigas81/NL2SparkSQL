from llm import get_llm

import pyspark
print(pyspark.__version__)

from pyspark.sql import SparkSession
from langchain_community.agent_toolkits import create_spark_sql_agent
from langchain_community.agent_toolkits.spark_sql.toolkit import SparkSQLToolkit
from langchain_community.utilities.spark_sql import SparkSQL
from dotenv import load_dotenv

# The famous Hadoop poem
poem_data = [
    ("Lotus", "Eat your own dog food", 2010, "Philosophical"),
    ("Pig", "Pig will eat anything", 2007, "Humorous"),
    ("Hive", "A place for bees to live", 2009, "Metaphorical"),
    ("HBase", "Hadoop database, not a face base", 2008, "Technical"),
    ("ZooKeeper", "Keeps all animals in order", 2008, "Organizational"),
    ("Spark", "Lightning fast, leaves MapReduce in the dark", 2014, "Proud"),
    ("MapReduce", "Divide and conquer, then reduce the source", 2004, "Strategic"),
    ("HDFS", "Files are split, across disks they sit", 2006, "Descriptive"),
    ("YARN", "Yet Another Resource Negotiator, not for knitting", 2013, "Acronymic"),
    ("Flume", "Data flows like water through a plume", 2011, "Fluidic"),
    ("Sqoop", "Between SQL and Hadoop, it makes a loop", 2012, "Connective"),
    ("Oozie", "Orchestrates workflows with ease", 2010, "Managerial"),
    ("Mahout", "Elephants learning, without a doubt", 2010, "AI-themed"),
    ("Tez", "Makes execution a breeze", 2013, "Efficient"),
    ("Kafka", "Streaming data, never a laugher", 2011, "Streaming"),
    ("Hue", "Makes Hadoop less blue", 2013, "UI-focused")
]


questions = [
    "How many projects are in the Hadoop ecosystem poem?",
 ]


def save_table(df, table_name="hadoop_poem"):
    # Get Spark session
    spark = df.sparkSession
    
    print("\nDatabases:")
    print("-" * 40)
    databases = spark.catalog.listDatabases()
    for db in databases:
        print(f"• {db.name}")
        if hasattr(db, 'description'):
            print(f"  Description: {db.description}")
        if hasattr(db, 'locationUri'):
            print(f"  Location: {db.locationUri[:50]}...")
    
    current_db = spark.catalog.currentDatabase()
    print(f"\nCurrent database: {current_db}")
    
    try:
            tables = spark.catalog.listTables(dbName=current_db)
            if not tables:
                print("  (No tables)")
                
                
            for table in tables:
                # Table type indicator
                table_type = "Temporary" if table.isTemporary else "Permanent"
                
                # Get more details
                details = []
                if hasattr(table, 'tableType'):
                    details.append(f"type: {table.tableType}")
                if hasattr(table, 'isTemporary'):
                    details.append("temp" if table.isTemporary else "perm")
                
                print(f"  {table_type} {table.name}")
                if details:
                    print(f"      ({', '.join(details)})")
                    
    except Exception as e:
            print(f"  Error accessing database '{db.name}': {e}")
            
    print(f"Saving as table '{table_name}'...")
    df.write.mode("overwrite").saveAsTable(table_name)
    print(f"Table '{table_name}' saved successfully.")

       

if __name__ == "__main__":
	# Load environment variables for GOOGLE_API_KEY
	load_dotenv()
	
	# Set up LLM
	llm = get_llm()
	
	table_name="hadoop_poem"
	# Remove table
	import shutil
	path=f"<home>/NL2SparkSQL/spark-warehouse/{table_name}"
	try:
		shutil.rmtree(path)
		print(f"Removed directory: {path}")
	except:
		print(f"Could not remove {path}")
	
	spark = SparkSession.builder \
    		.appName("HadoopPoem") \
		.getOrCreate()

	# Create DataFrame with schema
	df = spark.createDataFrame(poem_data, ["project", "description", "year", "theme"])
	# Creates a permanent managed table in the 'default' schema
	save_table(df, table_name=table_name)
	
	# Show the the entire Hadoop poem
	df.show(truncate=False)
	
	spark_sql = SparkSQL(schema="default", spark_session=spark)

	toolkit = SparkSQLToolkit(db=spark_sql, llm=llm)
	
	agent = create_spark_sql_agent(
    		llm=llm,
    		toolkit=toolkit,
    		verbose=True,
    		max_iterations=5,
    		handle_parsing_errors=True
	)

	print("\nAsking the Hadoop Poem questions I was never emotionally prepared to answer as professor of SDSA!\n")
	for i, question in enumerate(questions, 1):
	    print(f"\n{i}. Question: {question}")
	    print("_" * 40)
	    try: 
	    	result = agent.invoke({"input": question})
	    	print(f"Answer: {result['output']}")
	    except Exception as e:
	    	print(f"Error: {str(e)[:60]}...")  # Truncate long errors


	spark.stop() # Clean shutdown
