package org.example.realtime;

import org.example.DTO.EnrichedEvent;
import org.example.mapper.EnrichedEventMapper;
import org.example.DTO.Event;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.DTO.UnifiedProfile;
import org.example.common.ClickHouseSink;
import org.example.common.IdentityService;
import org.example.common.SessionEventSink;
import org.example.mapper.EventDeserializer;
import redis.clients.jedis.Jedis;

import java.util.Properties;

public class RealTimeJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Redis Initialisation
        Jedis redis = new Jedis("localhost", 6379);
        IdentityService identityService = new IdentityService(redis);

        // Kafka Configuration
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "identity-unifier");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "user-events",
                new SimpleStringSchema(),
                kafkaProps
        );

        DataStream<Event> parsedEvents = env.addSource(consumer)
                .map(new EventDeserializer()).name("Source Deserializer");

        // profile store
        parsedEvents
                .map(new IdentityResolver())
                .keyBy(UnifiedProfile::getUnifiedUserId)
                .addSink(new ClickHouseSink()).name("Profile Sink");

        // raw events
        parsedEvents
                .map(new EnrichedEventMapper())
                .keyBy(EnrichedEvent::getUnifiedUserId)
                .process(new SessionAssigner())
                .addSink(new SessionEventSink()).name("Session Sink");

        env.execute("User Identity Unification Job");
    }
}
