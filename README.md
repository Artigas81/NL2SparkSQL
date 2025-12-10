# NL2SparkSQL

This repository provides a hands-on example designed to help Master students explore **how Spark SQL works under the hood**. You’ll learn how **Spark translates queries into Logical and Physical plans** and executes them efficiently.  

The example uses **Natural Language Processing (NLP)** with **LangChain** to showcase a practical use case, bridging **big data processing** and **modern AI pipelines**. By working through this project, you will:  

- Understand **Spark’s query planning and execution flow**.  
- See the difference between **Logical Plans** and **Physical Plans**.  
- Apply **Spark SQL operations on real data**.  
- Integrate Spark workflows with **LangChain for NLP-driven tasks**.  

This is a perfect starting point for students in the course **`Safe Distributed System Architectures`** at University Rovira i Virgili. 

## Instructions

### Requirements
- **Python**: 3.12 or greater.
- **Java**: Java JDK/OpenJDK 21 or greater.

### 1. Installing and Setting Up Spark requirements
PySpark requires a Java Development Kit (JDK) to run.

**On Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install openjdk-21-jdk
java -version
```

**On macOS (using Homebrew):**
```bash
brew install openjdk@21
```

**On Windows:**
(Easiest option) Download and install Temurin JDK 21 from [Adoptium](https://adoptium.net/) or use `winget`:
```powershell
winget install Microsoft.OpenJDK.21
```
Ensure `JAVA_HOME` is set if Spark has trouble finding Java.

### 2. Installing the Virtual Environment
It is recommended to use a Python virtual environment to manage dependencies.

```bash
# Create a virtual environment
python3.12 -m venv sparkai-env

# Activate the environment
source sparkai-env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

You should activate the virtual environment every time you want to run the code.

### 3. Setting up Google AI Studio API Key
This project uses Google's Gemini models (default: `gemini-2.5-flash`). You need an API key from Google AI Studio.

1.  Go to [Google AI Studio](https://aistudio.google.com/).
2.  Create an API key.
3.  (Optional) You can set up billing in Google Cloud to get $300 in free credits if you need higher rate limits. The free tier (up to 10 requests per minute and 1500 requests/day for `gemini-2.5-flash`) might be sufficient just for basic testing.

Create a `.env` file in the root of the repository and add your key:

```bash
echo "GOOGLE_API_KEY=your_api_key_here" > .env
```

## Spark SQL & LangChain Examples

This repository provides hands-on examples to explore **Spark SQL**, **query execution plans**, and **natural language queries with LangChain**. We use a sample Hadoop poem dataset:

| project  | description                                      | year | theme         |
|----------|--------------------------------------------------|------|---------------|
| Lotus    | Eat your own dog food                            | 2010 | Philosophical |
| Pig      | Pig will eat anything                            | 2007 | Humorous      |
| Hive     | A place for bees to live                         | 2009 | Metaphorical  |
| HBase    | Hadoop database, not a face base                 | 2008 | Technical     |
| ZooKeeper| Keeps all animals in order                       | 2008 | Organizational|
| Spark    | Lightning fast, leaves MapReduce in the dark     | 2014 | Proud         |
| MapReduce| Divide and conquer, then reduce the source       | 2004 | Strategic     |
| HDFS     | Files are split, across disks they sit           | 2006 | Descriptive   |
| YARN     | Yet Another Resource Negotiator, not for knitting| 2013 | Acronymic     |
| Flume    | Data flows like water through a plume            | 2011 | Fluidic       |
| Sqoop    | Between SQL and Hadoop, it makes a loop          | 2012 | Connective    |
| Oozie    | Orchestrates workflows with ease                 | 2010 | Managerial    |
| Mahout   | Elephants learning, without a doubt              | 2010 | AI-themed     |
| Tez      | Makes execution a breeze                         | 2013 | Efficient     |
| Kafka    | Streaming data, never a laughter                 | 2011 | Streaming     |
| Hue      | Makes Hadoop less blue                           | 2013 | UI-focused    |

### Running the Examples

1. **`sqlonly.py`** – Execute SQL queries directly on the dataset.

```bash
python sqlonly.py
````

2. **`sqlexplain.py`** – Execute the same SQL queries using EXPLAIN to inspect Spark’s logical and physical execution plans.

```bash
python sqlexplain.py
```

3. **`test.py`** – Perform natural language queries. LangChain translates plain English queries into Spark SQL, executes them, and returns results.
```bash
python test.py
```
