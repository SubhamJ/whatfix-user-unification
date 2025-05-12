package org.example.mapper;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.example.DTO.EnrichedEvent;
import org.example.DTO.Event;
import org.example.common.IdentityService;
import redis.clients.jedis.Jedis;

public class EnrichedEventMapper extends RichMapFunction<Event, EnrichedEvent> {

    private transient Jedis redis;
    private transient IdentityService identityService;

    @Override
    public void open(Configuration parameters) {
        this.redis = new Jedis("localhost", 6379);
        this.identityService = new IdentityService(redis);
    }

    @Override
    public EnrichedEvent map(Event event) throws Exception {
        EnrichedEvent enriched = new EnrichedEvent();
        enriched.setUserId(event.getUserId());
        enriched.setCookieId(event.getCookieId());
        enriched.setDeviceId(event.getDeviceId());
        enriched.setPlatform(event.getPlatform());
        enriched.setTimestamp(event.getTimestamp());
        enriched.setEventType(event.getEventType());

        String unifiedId = identityService.resolveUnifiedUserId(event);
        enriched.setUnifiedUserId(unifiedId);

        return enriched;
    }

    @Override
    public void close() {
        if (redis != null) redis.close();
    }
}

