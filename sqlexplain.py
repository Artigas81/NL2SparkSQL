# Author: Marc Sanchez-Artigas
# Purpose: Explain the Spark SQL execution pipeline
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

# Corresponding SQL queries for each natural language question
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

def display_execution_plan(spark, sql_query, query_name=""):
    print(f"\n Execution Plan for: {query_name if query_name else sql_query[:50]}...")
    print("=" * 80)
    
    # Get the DataFrame without executing it
    df = spark.sql(sql_query)
    
    print("\n1. Logical plan:")
    print("-" * 40)
    try:
        logical_plan = df._jdf.queryExecution().logical().toString()
        print(logical_plan)
    except Exception as e:
        print(f"Could not display logical plan: {e}")
    
    print("\n2. Optimized logical plan (After Catalyst AFAIK):")
    print("-" * 40)
    try:
        optimized_plan = df._jdf.queryExecution().optimizedPlan().toString()
        print(optimized_plan)
    except Exception as e:
        print(f"Could not display optimized plan: {e}")
    
    print("\n3. Physical plan:")
    print("-" * 40)
    try:
        physical_plan = df._jdf.queryExecution().executedPlan().toString()
        # Format for better readability
        lines = physical_plan.split('\n')
        for line in lines:
            if line.strip():
                indent = len(line) - len(line.lstrip())
                indent_str = "  " * (indent // 2)
                print(f"{indent_str}{line.lstrip()}")
    except Exception as e:
        print(f"Could not display physical plan: {e}")
    
    # 4. Show Simple EXPLAIN output (traditional SQL style)
    print("\n 4. Explain output:")
    print("-" * 40)
    explain_df = spark.sql(f"EXPLAIN {sql_query}")
    explain_result = explain_df.collect()[0][0]
    
    # Parse and display in a cleaner format
    sections = explain_result.split("== ")
    for section in sections:
        if section.strip():
            lines = section.strip().split('\n')
            header = lines[0] if lines else ""
            if header:
                print(f"\n{header}:")
                print("-" * (len(header) + 1))
                for line in lines[1:]:
                    if line.strip():
                        print(f"  {line.strip()}")
    
    print("=" * 80)

def execute_and_show_results(spark, sql_query, question=""):
    """
    Execute a SQL query and display results with execution plan
    """
    print(f"\n{'='*80}")
    print(f"Question: {question}")
    print(f"SQL query: {sql_query}")
    print(f"{'='*80}")
    
    try:
        # First show the execution plan
        display_execution_plan(spark, sql_query, question)
        
        # Then execute and show results
        print("\nResults:")
        print("-" * 40)
        
        result_df = spark.sql(sql_query)
        
        row_count = result_df.count()
        
        if row_count == 1 and len(result_df.columns) == 1:
            # For single value results like COUNT, AVG
            value = result_df.collect()[0][0]
            print(f"Result: {value}")
            print(f"Rows returned: 1")
        else:
            # Show a preview of the results
            print(f"Total rows: {row_count}")
            if row_count > 0:
                print("\nFirst few rows:")
                result_df.show(5, truncate=False)
            
            # Show schema information
            print(f"\nResult schema:")
            for i, (col_name, col_type) in enumerate(zip(result_df.columns, result_df.dtypes)):
                print(f"  {i+1}. {col_name}: {col_type[1]}")
        
    except Exception as e:
        print(f"Error: {str(e)[:100]}...")
        import traceback
        traceback.print_exc()
    
    print(f"{'='*80}\n")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("HadoopPoemWithExplain") \
        .getOrCreate()

    # Create DataFrame with schema
    df = spark.createDataFrame(poem_data, ["project", "description", "year", "theme"])
    df.createOrReplaceTempView("hadoop_poem")

    # Show the entire Hadoop poem
    df.show(truncate=False)
    print(f"Total records: {df.count()}")
    
    # Execute each query with execution plan display
    for i, (question, sql_query) in enumerate(question_sql_map.items(), 1):
        print(f"\n\n{'#'*100}")
        print(f"Query #{i}: {question}")
        print(f"{'#'*100}")
        
        execute_and_show_results(spark, sql_query, question)
        
        # Pause between queries for readability
        if i < len(question_sql_map):
            input("\nPress Enter to see the next query analysis...")
    
    sample_query = "SELECT theme, COUNT(*) as count FROM hadoop_poem GROUP BY theme ORDER BY count DESC"
    
    # Different EXPLAIN modes
    explain_modes = {
        "EXPLAIN": f"EXPLAIN {sample_query}",
        "EXPLAIN EXTENDED": f"EXPLAIN EXTENDED {sample_query}",
        "EXPLAIN CODEGEN": f"EXPLAIN CODEGEN {sample_query}",
        "EXPLAIN COST": f"EXPLAIN COST {sample_query}"
    }
    
    for mode, explain_query in explain_modes.items():
        print(f"\n{mode}:")
        print("-" * 40)
        try:
            explain_df = spark.sql(explain_query)
            result = explain_df.collect()[0][0]
            print(result[:500] + "..." if len(result) > 500 else result)
        except Exception as e:
            print(f"  (Mode not supported: {e})")
    
    
    spark.stop()

