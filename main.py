import os
import json
import logging
from datetime import datetime
from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor
from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer, KinesisStreamsSink, PartitionKeyGenerator
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Online-Betting-Processing")

# Set up the Flink execution environment
env = StreamExecutionEnvironment.get_execution_environment()

# Define Application Properties
APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"

is_local = os.environ.get("IS_LOCAL", "false").lower() == "true"
if is_local:
    print("In Local Code...")
    APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"

    current_dir = os.path.dirname(os.path.abspath(__file__)).replace('\\','/')
    jar_path = f"{current_dir}/target/pyflink-dependencies.jar"

    # Check if JAR exists before adding it
    if os.path.exists(jar_path):
        env.add_jars(f"file:///{jar_path}")
        print(f"JAR loaded from: {jar_path}")
    else:
        print(f"JAR not found at: {jar_path}")
        print("Run 'mvn clean package' to build the JAR first")


def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            return json.load(file)
    else:
        logger.error(f"A file at {APPLICATION_PROPERTIES_FILE_PATH} was not found")
        return {}


def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]


# Generalized Schema for Deserialization
generalized_type_info = Types.ROW_NAMED(
    ["event_type", "game_id", "player_id", "bet_amount", "geo_location", "platform", "result", "multiplier", "timestamp"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.STRING(),
     Types.STRING(), Types.FLOAT(), Types.STRING()]
)

generalized_deserialization_schema = JsonRowDeserializationSchema.builder() \
    .type_info(generalized_type_info) \
    .build()

# Output Schema for Serialization
output_type_info = Types.ROW_NAMED(
    ["game_id", "player_id", "bet_amount", "game_result", "payout", "geo_location", "platform", "event_time", "is_high_value_bet"],
    [Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()]
)

output_serialization_schema = JsonRowSerializationSchema.builder() \
    .with_type_info(output_type_info) \
    .build()


# Stateful Processing Function
class BettingProcessFunction(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        """Initialize ListState to hold PlayerBet events."""
        self.bets_state = runtime_context.get_list_state(
            ListStateDescriptor("bets_state", Types.STRING())
        )

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        """Process incoming events."""
        # Convert Row to dict
        event = {
            "event_type": value.event_type,
            "game_id": value.game_id,
            "player_id": value.player_id,
            "bet_amount": value.bet_amount,
            "geo_location": value.geo_location,
            "platform": value.platform,
            "result": value.result,
            "multiplier": value.multiplier,
            "timestamp": value.timestamp,
        }
        event_type = event.get("event_type")
        game_id = event.get("game_id")

        if event_type == "PlayerBet":
            # Add PlayerBet event to state
            self.bets_state.add(json.dumps(event))
            logger.info(f"Stored PlayerBet event for game_id {game_id}: {event}")

        elif event_type == "GameResult":
            # Fetch all PlayerBet events for the given game_id
            bet_events = list(self.bets_state.get())

            if bet_events:
                logger.info(f"Processing GameResult for game_id {game_id} with {len(bet_events)} PlayerBet events")

                # Process each PlayerBet event
                for bet_event in bet_events:
                    bet_event = json.loads(bet_event)

                    # Business-level data validations
                    if bet_event["bet_amount"] <= 0:
                        logger.warning(f"Invalid bet amount: {bet_event['bet_amount']}")
                        continue

                    if "multiplier" not in event or event["multiplier"] <= 0:
                        logger.warning(f"Invalid multiplier in GameResult: {event}")
                        continue

                    # Transform and emit joined event
                    joined_event = Row(
                        game_id=game_id,
                        player_id=bet_event["player_id"],
                        bet_amount=bet_event["bet_amount"],
                        game_result=event["result"],
                        payout=round(bet_event["bet_amount"] * event["multiplier"], 2),
                        geo_location=bet_event["geo_location"],
                        platform=bet_event["platform"],
                        event_time=datetime.utcnow().isoformat(),
                        is_high_value_bet="Yes" if bet_event["bet_amount"] > 1000 else "No",
                    )
                    logger.info(f"Join successful, emitting event: {joined_event}")
                    yield joined_event

                # Clear the state for the processed game_id
                self.bets_state.clear()
            else:
                logger.warning(f"No matching bets found in state for GameResult: {event}")


# Main execution
def main():
    # Load application properties
    properties = get_application_properties()
    bets_stream = property_map(properties, "PlayerBetsStream")
    results_stream = property_map(properties, "GameResultsStream")
    output_stream = property_map(properties, "OutputStream")

    # Define Kinesis Consumers
    bets_consumer = FlinkKinesisConsumer(
        bets_stream["stream.name"],
        generalized_deserialization_schema,
        {"aws.region": bets_stream["aws"
                                   ".region"], "stream.initpos": "LATEST"},
    )
    results_consumer = FlinkKinesisConsumer(
        results_stream["stream.name"],
        generalized_deserialization_schema,
        {"aws.region": results_stream["aws.region"], "stream.initpos": "LATEST"},
    )

    # Define Kinesis Producer
    output_producer = KinesisStreamsSink.builder() \
        .set_stream_name(output_stream["stream.name"]) \
        .set_serialization_schema(output_serialization_schema) \
        .set_kinesis_client_properties({"aws.region": output_stream["aws.region"]}) \
        .set_partition_key_generator(PartitionKeyGenerator.random()) \
        .build()

    # Define Streams
    player_bets = env.add_source(bets_consumer)
    game_results = env.add_source(results_consumer)

    # Standardize Streams to Generalized Schema
    standardized_player_bets = player_bets.map(
        lambda event: Row(
            event_type=event.event_type,
            game_id=event.game_id,
            player_id=event.player_id,
            bet_amount=event.bet_amount,
            geo_location=event.geo_location,
            platform=event.platform,
            result=None,
            multiplier=None,
            timestamp=event.timestamp
        ),
        output_type=generalized_type_info
    )

    standardized_game_results = game_results.map(
        lambda event: Row(
            event_type=event.event_type,
            game_id=event.game_id,
            player_id=None,
            bet_amount=None,
            geo_location=None,
            platform=None,
            result=event.result,
            multiplier=event.multiplier,
            timestamp=event.timestamp
        ),
        output_type=generalized_type_info
    )

    # Union Streams
    combined_stream = (
        standardized_player_bets
        .union(standardized_game_results)
        .key_by(lambda x: x.game_id)
        .process(BettingProcessFunction(), output_type=output_type_info)
    )

    # Add Sink to output processed data
    combined_stream.sink_to(output_producer)
    # combined_stream.print()

    # Execute the Flink job
    logger.info("Starting Online Betting Stateful Processing")
    env.execute("Online Betting Stateful Processing")


if __name__ == "__main__":
    main()