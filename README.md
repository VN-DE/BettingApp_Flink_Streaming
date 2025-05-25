![Editor _ Mermaid Chart-2025-05-25-155028](https://github.com/user-attachments/assets/8366bd7f-4f82-4525-8aed-1e145bec3ee9)
# Online Betting System: End-to-End Data Processing Architecture

## Architecture Flow

The complete architecture builds a real-time data processing and analytics pipeline with the following components:

1. **Data Ingestion Layer**
   - PlayerBetsStreamInput (Kinesis Stream)
   - GameResultsStreamInput (Kinesis Stream)

2. **Stream Processing Layer**
   - Kinesis Data Analytics running Apache Flink application
   - Stateful processing and event correlation
  
     <img width="942" alt="Screenshot 2025-05-25 185023" src="https://github.com/user-attachments/assets/4c7b6231-debe-480c-94c1-32711408a034" />


3. **Data Delivery Layer**
   - ProcessedBetsStreamOutput (Kinesis Stream)
  
     <img width="938" alt="Screenshot 2025-05-25 185115" src="https://github.com/user-attachments/assets/241d829d-06dd-4682-b5dc-5cf614ed7348" />

   - Kinesis Firehose for data delivery to S3
     
    <img width="941" alt="Screenshot 2025-05-25 185744" src="https://github.com/user-attachments/assets/827d6d91-b291-4ace-9aef-1fbec7b2d106" />

     

4. **Data Storage Layer**
   - S3 buckets organized as a data lake

5. **Data Discovery & Catalog Layer**
   - AWS Glue Crawler to discover schema
   - AWS Glue Data Catalog to maintain metadata

    <img width="949" alt="glue_table" src="https://github.com/user-attachments/assets/d30c6cf1-8541-4c6f-bbd6-7d82cea5bf1a" />


6. **Analytics Layer**
   - Amazon Athena for interactive SQL queries
   - Connection to BI tools and applications
  
   <img width="947" alt="athena" src="https://github.com/user-attachments/assets/520add8a-150b-4fea-ba50-e6135ee705a1" />
  

# Online Betting Processing System Documentation

## Project Overview

This project implements a real-time data processing system for an online betting platform using Apache Flink and AWS Kinesis. The system processes two main event types - player bets and game results - joining them together to calculate payouts and provide enriched betting analytics in real-time.

## Architecture and Services

### Core Services

1. **AWS Kinesis Streams**:
   - Used for real-time data ingestion and transportation
   - Three streams are configured:
     - `PlayerBetsStreamInput`: Receives player betting events
     - `GameResultsStreamInput`: Receives game outcome events  
     - `ProcessedBetsStreamOutput`: Outputs joined and processed betting data

2. **Apache Flink (PyFlink)**:
   - Stream processing framework that provides stateful computation
   - Used to join bet events with corresponding game results
   - Maintains state to correlate events that arrive at different times
   - Runs on AWS Kinesis Data Analytics (KDA) for serverless operation

3. **AWS Kinesis Data Analytics (KDA)**:
   - Managed service running the Flink application
   - Handles scaling, availability, and infrastructure management

### Data Flow

1. Betting events are published to `PlayerBetsStreamInput`
2. Game result events are published to `GameResultsStreamInput`
3. The Flink application:
   - Reads from both input streams
   - Joins events based on game_id
   - Enriches data with additional calculations
   - Validates data quality
   - Outputs processed results to `ProcessedBetsStreamOutput`
4. Firehose takes Processed data and dumps it into S3.
5. Glue Crawler scans the S3 bucket to update metadata into Glue table.
6. Athena is used to query on S3 data using Glue table metadata.

## Code Files Explanation

### 1. `main.py`

**Purpose**: Main PyFlink application script containing the core stream processing logic.

  ![Editor _ Mermaid Chart-2025-05-24-135123](https://github.com/user-attachments/assets/26e28290-b686-4da0-927c-f80c7a645f8a)


**Key Components**:

- **Environment Setup**:
  ```python
  env = StreamExecutionEnvironment.get_execution_environment()
  ```
  Creates the Flink execution environment needed to define sources, transformations, and sinks.

- **Configuration Management**:
  ```python
  def get_application_properties():
      if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
          with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
              return json.load(file)
  ```
  Loads application configuration from a JSON file with different paths for local vs. cloud deployment.

- **Schema Definitions**:
  ```python
  generalized_type_info = Types.ROW_NAMED(
      ["event_type", "game_id", "player_id", "bet_amount", "geo_location", "platform", "result", "multiplier", "timestamp"],
      [Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.STRING(),
       Types.STRING(), Types.FLOAT(), Types.STRING()]
  )
  ```
  Defines the data schemas for serialization and deserialization to/from Kinesis.

- **Stateful Processing**:
  ```python
  class BettingProcessFunction(KeyedProcessFunction):
      def open(self, runtime_context: RuntimeContext):
          self.bets_state = runtime_context.get_list_state(
              ListStateDescriptor("bets_state", Types.STRING())
          )
  ```
  Custom stateful processing function that:
  - Stores player bet events in state
  - Joins with corresponding game results when they arrive
  - Performs business logic for calculating payouts
  - Validates data quality
  - Enriches events with additional fields

- **Stream Processing Pipeline**:
  ```python
  combined_stream = (
      standardized_player_bets
      .union(standardized_game_results)
      .key_by(lambda x: x.game_id)
      .process(BettingProcessFunction(), output_type=output_type_info)
  )
  ```
  Builds the processing pipeline by:
  - Standardizing both event streams to a common schema
  - Unioning the streams
  - Keying by game_id 
  - Applying the stateful process function
  - Producing enriched output events

### 2. `application_properties.json`

**Purpose**: Configuration file for the Flink application defining stream names and regions.

**Key Components**:

- **Flink Runtime Options**:
  ```json
  {
    "PropertyGroupId": "kinesis.analytics.flink.run.options",
    "PropertyMap": {
      "python": "main.py",
      "jarfile": "lib/pyflink-dependencies.jar"
    }
  }
  ```
  Specifies the main Python file and dependency JAR for the Flink application.

- **Stream Configurations**:
  ```json
  {
    "PropertyGroupId": "PlayerBetsStream",
    "PropertyMap": {
        "stream.name": "PlayerBetsStreamInput",
        "aws.region": "us-east-1"
    }
  }
  ```
  Defines the stream names and AWS regions for all input and output streams.

### 3. `mock_data_gen.py`

**Purpose**: Utility script to generate test data for the application.

**Key Components**:

- **Data Generation Functions**:
  ```python
  def generate_player_bet(game_id):
      return {
          "event_type": "PlayerBet",
          "game_id": game_id,
          "player_id": f"player-{random.randint(1000, 9999)}",
          "bet_amount": round(random.uniform(5, 5000), 2),
          "geo_location": random_geo_location(),
          "platform": random_platform(),
          "timestamp": datetime.utcnow().isoformat(),
      }
  ```
  Functions to create realistic mock data for player bets and game results.

- **Invalid Data Generation**:
  ```python
  def generate_invalid_player_bet():
      return {
          "event_type": "PlayerBet",
          "game_id": f"game-{random.randint(1, 1000)}",
          "player_id": None,  # Invalid player_id
          "bet_amount": -random.uniform(1, 100),  # Invalid bet amount
          # ...
      }
  ```
  Generates edge cases with invalid data to test the system's robustness.

- **Kinesis Integration**:
  ```python
  def publish_event_to_kinesis(stream_name, event):
      kinesis_client.put_record(
          StreamName=stream_name,
          Data=json.dumps(event),
          PartitionKey=event["game_id"],
      )
  ```
  Publishes events to Kinesis streams using the Boto3 AWS SDK.

### 4. `pom.xml`

**Purpose**: Maven project configuration file that manages dependencies and build settings for the Java/Scala components required by PyFlink.

**Key Components**:

- **Project Dependencies**:
  ```xml
  <dependencies>
      <dependency>
          <groupId>org.apache.flink</groupId>
          <artifactId>flink-connector-kinesis</artifactId>
          <version>${aws.connector.version}</version>
      </dependency>
  </dependencies>
  ```
  Defines the Flink Kinesis connector dependency needed for stream processing.

- **Build Configuration**:
  ```xml
  <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-shade-plugin</artifactId>
      <!-- ... -->
  </plugin>
  ```
  Uses the Maven Shade plugin to create a fat JAR containing all dependencies.

- **Assembly Configuration**:
  ```xml
  <plugin>
      <artifactId>maven-assembly-plugin</artifactId>
      <!-- ... -->
  </plugin>
  ```
  Packages the application code and dependencies into a deployable format.

### 5. `assembly.xml`

**Purpose**: Maven Assembly plugin configuration that defines the final zip structure for deployment.

**Key Components**:

- **File Inclusions**:
  ```xml
  <fileSet>
      <directory>${project.basedir}</directory>
      <outputDirectory>/</outputDirectory>
      <includes>
          <include>main.py</include>
      </includes>
  </fileSet>
  ```
  Specifies which files to include in the final package (Python code and compiled JAR).

## Technical Design Rationale

### 1. Stateful Processing Approach

**Rationale**: Player bets and game results are separate events that arrive asynchronously but need to be joined.

- **Implementation**: The `BettingProcessFunction` uses Flink's stateful processing to:
  - Store player bet events in a ListState object
  - Wait for the corresponding game result event
  - Process and join them when all necessary data is available
  - Clear the state after processing to conserve memory

- **Benefits**:
  - Handles out-of-order events naturally
  - Enables event correlation across time
  - Provides exactly-once processing guarantees

### 2. Stream Union Pattern

**Rationale**: Joining two streams with a common key is more efficient using a union operation followed by keying.

- **Implementation**:
  ```python
  combined_stream = (
      standardized_player_bets
      .union(standardized_game_results)
      .key_by(lambda x: x.game_id)
      .process(BettingProcessFunction(), output_type=output_type_info)
  )
  ```

- **Benefits**:
  - Simpler processing logic as all events flow through a single function
  - More efficient use of Flink's partitioning and state backend
  - Better parallelism as keyed state is distributed

### 3. Data Validation & Error Handling

**Rationale**: Real-world streaming data may contain errors or inconsistencies.

- **Implementation**:
  ```python
  # Business-level data validations
  if bet_event["bet_amount"] <= 0:
      logger.warning(f"Invalid bet amount: {bet_event['bet_amount']}")
      continue
  ```

- **Benefits**:
  - Prevents downstream processing errors
  - Provides observability through logging
  - Maintains data quality in the output stream

### 4. Configurability Via External Properties

**Rationale**: Streamline deployment across environments without code changes.

- **Implementation**: Using an externalized `application_properties.json` file that's loaded at runtime:
  ```python
  properties = get_application_properties()
  bets_stream = property_map(properties, "PlayerBetsStream")
  ```

- **Benefits**:
  - Easy to configure for different environments (dev, test, prod)
  - Simplifies deployment process
  - Follows separation of concerns principle

## Deployment Considerations

### Packaging

The application is packaged into a standard format using Maven:
1. Python code (`main.py`) at the root
2. Dependencies JAR (`pyflink-dependencies.jar`) in the `lib` directory

### AWS Kinesis Data Analytics Setup

Required configuration for the KDA application:
1. IAM roles with permissions for Kinesis streams
2. Application properties pointing to the correct streams
3. Appropriate resource allocation (parallelism, memory)

### Local Development

The code includes provisions for local testing:
```python
is_local = os.environ.get("IS_LOCAL", "false").lower() == "true"
if is_local:
    APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"
    env.add_jars(f"file:///Users/shashankmishra/Desktop/pyflink-dependencies.jar")
```

## Data Flow Optimization

1. **Standardization Before Union**:
   ```python
   standardized_player_bets = player_bets.map(
       lambda event: Row(
           event_type=event.event_type,
           # ...
       ),
       output_type=generalized_type_info
   )
   ```
   Events are standardized to a common schema before union to simplify downstream processing.

2. **Efficient State Management**:
   ```python
   # Clear the state for the processed game_id
   self.bets_state.clear()
   ```
   State is cleared after processing to prevent memory leaks and improve performance.




