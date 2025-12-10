import pyspark
print(f"PySpark version: {pyspark.__version__}")

from pyspark.sql import SparkSession

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


question_sql_map = {
    "How many projects are in the Hadoop ecosystem poem?": 
        "SELECT COUNT(*) as project_count FROM hadoop_poem",
    "Which projects were created before 2010?": 
        "SELECT project, year FROM hadoop_poem WHERE year < 2010 ORDER BY year",
    "What are all the different themes mentioned?": 
        "SELECT DISTINCT theme FROM hadoop_poem ORDER BY theme",
    "Find projects with 'data' in their description": 
        "SELECT project, description FROM hadoop_poem WHERE LOWER(description) LIKE '%data%'",
    "List projects in alphabetical order": 
        "SELECT project FROM hadoop_poem ORDER BY project",
    "What is the average year of creation?": 
        "SELECT AVG(year) as avg_year FROM hadoop_poem",
    "Show me projects with humorous or philosophical themes": 
        "SELECT project, theme, description FROM hadoop_poem WHERE theme IN ('Humorous', 'Philosophical')",
    "Which project description mentions 'elephants'?": 
        "SELECT project, description FROM hadoop_poem WHERE LOWER(description) LIKE '%elephant%'",
    "Count projects by theme": 
        "SELECT theme, COUNT(*) as count FROM hadoop_poem GROUP BY theme ORDER BY count DESC",
    "What SQL query would find the newest project?": 
        "SELECT project, year FROM hadoop_poem WHERE year = (SELECT MAX(year) FROM hadoop_poem)"
}

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("HadoopPoem") \
        .getOrCreate()

    # Create DataFrame with schema
    df = spark.createDataFrame(poem_data, ["project", "description", "year", "theme"])
    df.createOrReplaceTempView("hadoop_poem")

    # Show the entire Hadoop poem
    df.show(truncate=False)
    
    print("\nAsking the Hadoop Poem questions I was never emotionally prepared to answer as professor of SDSA!\n")
    print("=" * 80)
    
    for i, (question, sql_query) in enumerate(question_sql_map.items(), 1):
        print(f"\n{i}. Question: {question}")
        print(f"   Generated SQL: {sql_query}")
        print("-" * 40)
        
        try:
            # Execute the SQL query directly
            result_df = spark.sql(sql_query)
            
            # Format the output nicely
            if result_df.count() == 1 and len(result_df.columns) == 1:
                # For single value results like COUNT, AVG
                value = result_df.collect()[0][0]
                print(f"   Answer: {value}")
            else:
                # For tabular results
                print("   Answer:")
                result_df.show(truncate=False)
                
        except Exception as e:
            print(f"   Error: {str(e)[:80]}...")
    
    print("\n" + "=" * 80)
    
    
    # Optional: Show table schema for reference
    print("\n Table Schema:")
    spark.sql("DESCRIBE hadoop_poem").show(truncate=False)
    
    spark.stop()
