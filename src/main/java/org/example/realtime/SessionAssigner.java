package org.example.realtime;

import org.example.DTO.EnrichedEvent;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class SessionAssigner extends KeyedProcessFunction<String, EnrichedEvent, EnrichedEvent> {

    private transient ValueState<String> currentSessionId;
    private transient ValueState<Long> lastSeenTimestamp;
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionAssigner.class);

    private static final long INACTIVITY_TIMEOUT = Time.hours(12).toMilliseconds();

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<String> sessionIdDesc = new ValueStateDescriptor<>("sessionId", String.class);
        ValueStateDescriptor<Long> lastSeenDesc = new ValueStateDescriptor<>("lastSeen", Long.class);
        currentSessionId = getRuntimeContext().getState(sessionIdDesc);
        lastSeenTimestamp = getRuntimeContext().getState(lastSeenDesc);
    }

    @Override
    public void processElement(EnrichedEvent event, Context ctx, Collector<EnrichedEvent> out) throws Exception {
        Long eventTimestamp = Long.parseLong(event.getTimestamp());
        String currentSession = currentSessionId.value();
        Long lastSeen = lastSeenTimestamp.value();
        boolean resetSession = false;

        LOGGER.info("Event:{}, currentTimestamp:{}, lastSeen:{}", event.toString(), currentSession, lastSeen);

        // inactivity timeout
        if (currentSession == null || lastSeen == null || (eventTimestamp - lastSeen) > INACTIVITY_TIMEOUT) {
            resetSession = true;
        }

        // logout event
        boolean isLogout = "log_out".equalsIgnoreCase(event.getEventType());

        if (resetSession || currentSession == null) {
            currentSession = UUID.randomUUID().toString();
            currentSessionId.update(currentSession);
        }

        // Stamp with current session
        event.setSessionId(currentSession);
        event.setEventTimestamp(eventTimestamp);
        out.collect(event);

        // end session in case of logout
        if (isLogout) {
            currentSessionId.clear();
            lastSeenTimestamp.clear();
        } else {
            lastSeenTimestamp.update(eventTimestamp);
        }
    }
}

