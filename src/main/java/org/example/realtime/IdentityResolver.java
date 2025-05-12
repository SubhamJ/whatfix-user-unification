package org.example.realtime;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.example.DTO.Event;
import org.example.DTO.UnifiedProfile;
import org.example.common.IdentityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

// resolver using identity service
public class IdentityResolver extends RichMapFunction<Event, UnifiedProfile> {

    private transient Jedis redis;
    private transient IdentityService identityService;
    private static final Logger LOGGER = LoggerFactory.getLogger(IdentityResolver.class);

    @Override
    public void open(Configuration parameters) {
        this.redis = new Jedis("localhost", 6379);
        this.identityService = new IdentityService(redis);
    }

    @Override
    public UnifiedProfile map(Event event) throws Exception {
        LOGGER.info("Resolving identity for event: {}", event);
        String unifiedId = identityService.resolveUnifiedUserId(event);

        UnifiedProfile profile = new UnifiedProfile();
        profile.setUnifiedUserId(unifiedId);
        profile.addIdentifier(event);
        profile.updateMetadata(event);

        identityService.cacheIdentifiers(event, unifiedId);
        return profile;
    }

    @Override
    public void close() {
        if (redis != null) redis.close();
    }
}

