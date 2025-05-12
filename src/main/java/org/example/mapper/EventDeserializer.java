package org.example.mapper;

import org.example.DTO.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

public class EventDeserializer implements MapFunction<String, Event> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Event map(String value) throws Exception {
        return objectMapper.readValue(value, Event.class);
    }
}
