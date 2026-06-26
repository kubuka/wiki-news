# Real-Time Wiki Trend Monitor 🌍
Real-Time Wikipedia Trend Monitor is a streaming pipeline designed to "listen" to live, global Wikipedia edit activity. Instead of building slow, traditional data warehouses, this project focuses on an Event-Driven architecture, Low Latency, and strict cloud cost optimization (FinOps).

The system pulls raw logs directly from the Wikimedia EventStreams API, buffers them in Kafka, and aggregates them on the fly into 1-minute Tumbling Windows using Spark. Rather than dumping every single raw event to the cloud, Spark pushes only the final, aggregated statistics straight to BigQuery. The entire pipeline is capped off with a live, interactive dashboard.

### Key Features
- ***Real-Time Streaming*** - Processing data on the fly using Apache Kafka and PySpark Structured Streaming, completely bypassing slow disk-based storage.

- ***Event-Time Aggregation*** - Grouping edits into 1-minute Tumbling Windows based on actual Event Time, using Watermarks to handle late-arriving data.


- ***Interactive Dashboard*** - A Streamlit application with background auto-refresh, allowing users to track trends across different language editions and watch the global "Bots vs. Humans" editing war in real-time.
---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Tech Stack](#tech-stack)
3. [Streaming Pipeline](#streaming-pipeline)
4. [Stream Processing Logic](#stream-processing-logic)
5. [Dashboard](#dashboard)
6. [Dockerization](#dockerization)
7. [Summary and Key Learnings](#summary-and-key-learnings)
8. [Before You Run](#before-you-run)

---
## Project Structure
```text
.
├── checkpoints/                  # Spark Structured Streaming state and checkpoint storage
├── creds/                        # Directory for GCP Service Account JSON keys
├── dashboard/                    # Visualization layer
│   └── app.py                    # Streamlit real-time dashboard application
├── scripts/                      # Core pipeline logic
│   ├── stream_processor.py       # PySpark Structured Streaming job (Kafka -> BigQuery)
│   └── stream_producer.py        # Python Kafka producer fetching live data from Wikipedia
├── docker-compose.yml            # Multi-container setup (Kafka, Producer, Spark, Dashboard)
├── Dockerfile.dashboard          # Container setup for Streamlit
├── Dockerfile.producer           # Python container for the Wikipedia listener
├── Dockerfile.spark              # Container with Python, OpenJDK, and PySpark
├── pyproject.toml                # Python project dependency management configurations
├── uv.lock                       # Lockfile ensuring strict Python package reproducibility
```
---
## Tech Stack

* ***Kafka*** (KRaft) - Handles message brokering and buffers the live Wikipedia edit stream. Used the modern KRaft mode to drop the Zookeeper dependency, saving RAM and simplifying the architecture.

* ***PySpark*** (Structured Streaming) - Processes the unbounded data stream in RAM. It applies 1-minute Tumbling Windows and Watermarks to aggregate statistics on the fly before sending them to the cloud.

* ***Google BigQuery*** - Cloud Data Warehouse. It receives only the final, aggregated statistics (not raw events) to keep query and storage costs low. Tables are partitioned and clustered for query optimization.

* ***Streamlit & Plotly*** - Used to build the interactive, web dashboard. Plotly handles the data visualizations, while Streamlit's `@st.fragment `manages the live auto-refresh without reloading the whole page.

* ***Docker*** - Containerizes the entire stack (Kafka, Producer, Spark, Dashboard), allowing the system to run with a single command.
---
## Streaming Pipeline
The architecture's flow moves data from a live public API to a cloud warehouse, prioritizing low latency and avoiding unnecessary disk writes for raw data.

```text
[ Wikimedia EventStreams API ]
            │
            │ (Server-Sent Events / SSE)
            ▼
[ Python Producer ]
            │
            │ (Filters noise, pushes JSON to Kafka)
            ▼
[ Apache Kafka (Topic: wiki_raw_stream) ]
            │
            │ (Buffers unbounded stream)
            ▼
[ PySpark Structured Streaming ]
            │
            │ (Parses JSON, applies 1-min Tumbling Windows)
            ▼
[ Google BigQuery ]
            │
            │ (Reads aggregated tables via SQL)
            ▼
[ Streamlit Dashboard ]
```
### Pipeline Stages

- ***Ingestion*** - Python script connects to the Wikimedia EventStreams API. It filters out non-article edits (like log entries or user page updates) and pushes only relevant JSONs to Kafka. The server_name (e.g., en.wikipedia.org) is used as the Kafka message key to ensure all edits from a specific language edition go to the same partition in chronological order.
```python
    p.produce(
            KAFKA_TOPIC,
            key=data["server_name"].encode("utf-8"),
            value=json.dumps(data).encode("utf-8"),
            callback=delivery_report,
            )
```

- ***Buffering*** - Kafka acts as a decoupling layer. It absorbs sudden spikes in traffic (e.g. Breaking News events) so the Spark processor is not overwhelmed. Data is kept in Kafka's memory/disk until Spark is ready to consume it.

- ***Processing*** - PySpark Structured Streaming reads the raw bytes from Kafka, casts them to strings, and parses the JSON. It groups the stream into 1-minute windows based on the event timestamp from Wikipedia, not the processing time.

- ***Loading*** - Instead of writing raw files to Google Cloud Storage, Spark uses the foreachBatch method and `writeMethod="direct"` to push only the final aggregated rows directly into BigQuery.

- ***Serving*** - Streamlit dashboard queries these lightweight, aggregated tables in BigQuery every 60 seconds to render the live charts.

---
## Stream Processing Logic
The core of the pipeline relies on PySpark Structured Streaming to transform stream of raw JSON events into structured, aggregated tables. Instead of processing each event individually, Spark groups them into fixed time intervals.

### Event-Time Tumbling Windows
Wikipedia edits are grouped into 1-minute Tumbling Windows. These windows are strictly based on event_time (the exact moment the edit occurred on Wikipedia servers), not the processing time when Spark received the message. This ensures that statistics are accurate even if Kafka or Spark experiences a slight delay. Each window aggregates the total edits, bot edits, and human edits per Wikipedia domain.

### Watermarks and Late Data
In streaming systems, events can arrive out of order or late. To prevent Spark from holding windows open in memory indefinitely (which would cause RAM leaks), a Watermark of 1 minute is applied.

- Spark tracks the maximum event_time seen so far.
- If an edit arrives with a timestamp older than the current maximum minus 1 minute, it is considered too late and is dropped.
- Once the watermark passes the end of a 1-minute window, Spark finalizes the aggregation, writes the result to BigQuery, and safely purges the window state from memory.

```python
    with_time_df.withWatermark("event_time", "1 minute")
    .groupBy(window(col("event_time"), "1 minute"), col("server_name"))
```

### BigQuery Direct Write `(foreachBatch)`
Writing streaming data to BigQuery can be problematic due to connector version conflicts and dependency hell (specifically between Hadoop libraries and Google Cloud Storage).

To bypass this, the `foreachBatch micro-batch processing pattern` is used. When a 1-minute window closes, Spark passes the finalized DataFrame to a custom Python function. This function writes the batch directly to BigQuery using the `writeMethod="direct"` option (BigQuery Storage Write API). This approach completely omits the Hadoop file system layer, requiring no temporary files in Google Cloud Storage and ensuring high reliability.

```python
def save_global_stats(batch_df, batch_id):
        if not batch_df.isEmpty():
            (
                batch_df.write.format("bigquery")
                .option(
                    "table", f"{GCP_PROJECT_ID}.wikipedia_streaming.wiki_global_stats"
                )
                .option(
                    "writeMethod", "direct"
                )  # xddd wystarczylo zmienic na direct zabije sie a nie jakis bucket
                .mode("append")
                .save()
            )
```
---
## Dashboard
The serving layer is implemented as an interactive web application built with Streamlit and Plotly. It queries the aggregated tables in BigQuery directly, transforming cloud data into live visual insights.

### Real-Time Refresh and Caching
To provide a live experience without overloading the BigQuery backend, the dashboard utilizes two key Streamlit features:

- `@st.fragment(run_every=timedelta(minutes=1))` - The charts auto-refresh in the background every minute. This prevents the entire web application from blinking or losing user interaction state.

- `@st.cache_data(ttl=60)` - When users interact with UI elements (like changing the language dropdown), cached data is returned instantly from RAM. BigQuery is only queried once per minute, strictly adhering to FinOps principles.

### Dashboard Sections
 
The interface is divided into three distinct sections:

1. `Bots vs Humans` - Displays the global editing ratio in a Donut Chart, a Stacked Bar Chart comparing the Top 5 most active Wikipedia domains, and a 24-hour Line Chart showing minute-by-minute trends.

2. `Global Hot Topics` - Highlights the top 3 most edited articles worldwide in the last hour.

3. `Trending by Language` - A dynamic dropdown menu populated with active Wikipedia domains. It displays a horizontal Bar Chart of the Top 10 trending articles for the selected language. The data is fetched using a single SQL query with a ROW_NUMBER() Window Function to efficiently get the top articles for all domains at once.



---
## Dockerization

To ensure total environment reproducibility, the entire stack is containerized using Docker Compose. This solves the complex dependency conflicts between Python, Java, and PySpark, allowing the pipeline to run on any OS with a single command.

```text
[ Kafka (KRaft) ] ◄─── (healthcheck) ─── [ Kafka-Init ]
       ▲                                        │
       │                                        │ (creates topic & exits)
       │ (produces JSON)                        │
       │                                        ▼
[ Wiki-Producer ] ◄──────────────────── (dependency chain)
       │
       │ (dependency chain)
       ▼
[ Spark-Processor ] ─────(Direct Write)─────► [ Google BigQuery ]
                                                        ▲
[ Wiki-Dashboard ] ◄───────(SQL Queries)────────────────┘
```

### Container Orchestration
The `docker-compose.yml` file establishes a strict dependency chain to prevent hiccups during startup:

- Kafka boots first in KRaft mode. A healthcheck continuously pings its internal API.
- Kafka-Init waits until Kafka is fully healthy, creates the `wiki_raw_stream` topic, and exits.
- Wiki-Producer starts only after the topic is successfully created (`service_completed_successfully`).
- Spark-Processor waits for the producer to start, ensuring data is flowing before it begins processing.

### Secrets 
Sensitive data and environment variables are handled using the `.env` file. The file is bind-mounted into the containers at runtime, keeping credentials out of the built Docker images and ensuring clean separation between code and configuration.

---
## Summary and Key Learnings
Building a real-time, cloud-connected streaming pipeline from scratch was a completely different beast compared to traditional batch processing. Here are the main challenges and what I took away from them.

### Key Challenges

1. **Learning Kafka From Scratch**

    Since this was my first deep dive into Kafka, just understanding the internal mechanics of brokers, controllers, and topics was a challenge. The trickiest part was figuring out the difference between `KAFKA_LISTENERS` (where Kafka actually listens) and `KAFKA_ADVERTISED_LISTENERS` (the address it hands out to clients). Getting this right was crucial so that both internal Docker containers and the local host machine could talk to the broker without throwing connection errors.

2. **Bridging Kafka and PySpark**

    Connecting PySpark Structured Streaming to Kafka meant dealing with raw binary payloads. Kafka sends bytes, so the data had to be manually cast to strings and parsed via `from_json` using a strict schema. On top of that, managing stateful aggregations (1-minute Tumbling Windows) required a solid understanding of Event-Time processing and Watermarks to keep the RAM from leaking while still catching delayed data.

3. **Spark-BigQuery Permissions & Dependency Hell (Crème de la crème)**

    Integrating Spark with BigQuery turned out to be the ultimate boss fight.

    On the authentication side, I initially struggled to understand GCP IAM and the difference between local Application Default Credentials (ADC) and Service Account JSON keys. While ADC worked like magic locally, moving the app to Docker broke it completely because the isolated container couldn't access my local hidden credential files. I had to learn how to provision a dedicated Service Account, assign it the correct BigQuery roles, and explicitly mount the JSON key into the Spark container.

    Then came the actual Dependency Hell. The standard streaming connector kept throwing `NoSuchMethodError` exceptions due to some Guava conflicts between Hadoop and Google Cloud dependencies. To make matters worse, the Hadoop layer didn't even know how to handle the `gs://` protocol for temporary files.

    The breakthrough came from bypassing the standard streaming sink entirely. By switching to the `foreachBatch micro-batch `pattern and forcing `writeMethod="direct"`, the pipeline uses the BigQuery Storage Write API directly. This completely skips the Hadoop file system layer, kills the dependency conflicts, and removes the need for temporary GCS buckets entirely.

    *If you want to see the battle scars, just look at the commented-out code iterations in the SparkSession config:*
    ```python
    SparkSession.builder.appName("WikiStreamProcessor")
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                    "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
                    # "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22",<-- Failed attempt
                ]
            ),
        )
        .config("parentProject", GCP_PROJECT_ID)
        # jednak adc wystarczy, na produkcji klucze sie przydadzą
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            "/opt/spark/creds/wiki-spark-key.json",
        )
        # .config(
        #     "spark.hadoop.fs.gs.impl",
        #     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",<-- Failed attempt
        # )
        # .config(
        #     "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        #     "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",<-- Failed attempt
        # ) # to do zapisywania gs:// ale nie trzeba bo jest direct
    ```

### Key Takeaways
- ***FinOps in Streaming*** - Streaming straight to a cloud warehouse doesn't mean streaming raw rows. Aggregating in RAM and pushing only final micro-batches is the only way to keep cloud costs down.

- ***Kafka is No Longer a Black Box*** - Before this project, Kafka was just a buzzword to me. Going through the pain of configuring KRaft, listeners, and internal Docker networking taught me how brokers actually communicate under the hood.

- ***GCP Maze*** - I got a solid grasp of Google Cloud Platform, specifically BigQuery. I learned the hard way how ADC works compared to Service Account JSON keys, and how to optimize cloud warehouse costs using table partitioning and clustering.

- ***Production Mindset*** - I realized that writing a working Python script is just 20% of the job. Packaging it into containers, handling network dependencies, securing credentials, and making the pipeline resilient to restarts is what actually makes it an engineering project.

+ (debugging spark all day long is not so scary anymore)
---
## Before You Run

### 1. Google Cloud Setup (BigQuery & Service Account)
The pipeline writes to BigQuery, so you need a dedicated Service Account and the correct table schema.
- Go to the GCP Console and create a new project. Note your Project ID (e.g., wiki-news-12345).
- Navigate to IAM & Admin -> Service Accounts and create a new Service Account (e.g., wiki-spark).
- Grant this Service Account the following roles: BigQuery Data Editor and BigQuery Job User.
- Navigate to the Keys tab for this Service Account, add a new JSON key, download it, rename it to `wiki-spark-key.json`, and place it in the `creds/` directory of this project.


### 2. Create BigQuery Tables
Open the BigQuery console, create a dataset named `wikipedia_streaming` (Location: EU), and run the following SQL to create the optimized tables:
```sql
-- Table for global statistics
CREATE OR REPLACE TABLE `wikipedia_streaming.wiki_global_stats` (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  server_name STRING,
  total_edits INT64,
  bot_edits INT64,
  human_edits INT64
)
PARTITION BY TIMESTAMP_TRUNC(window_start, HOUR)
CLUSTER BY server_name
OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY));

-- Table for hot topics
CREATE OR REPLACE TABLE `wikipedia_streaming.wiki_hot_topics` (
  window_start TIMESTAMP,
  window_end TIMESTAMP,
  server_name STRING,
  page_title STRING,
  edit_count INT64
)
PARTITION BY TIMESTAMP_TRUNC(window_start, HOUR)
CLUSTER BY server_name, page_title
OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY));
```

### 3. Configure Environment Variables
Create a `.env` file in the root directory of the project and populate it with the following variables:
```env
# Required by the Python producer to connect to Wikipedia API
USER_AGENT="RealTimeWikipediaTrendMonitor/1.0 (your_email@example.com)"

# Your GCP Project ID
GCP_PROJECT_ID="your-gcp-project-id"

# Path to the Service Account JSON key inside the Docker containers
GCP_CREDS_PATH="/opt/spark/creds/wiki-spark-key.json"
```

### 4.Launch the Pipeline
```text
docker compose up -d --build
```

have fun! :)
