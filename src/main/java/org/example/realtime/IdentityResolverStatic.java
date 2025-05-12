package org.example.realtime;

import org.example.DTO.Event;
import org.example.DTO.UnifiedProfile;
import org.apache.flink.api.common.functions.MapFunction;
import java.util.UUID;

//IdentityResolver without state
public class IdentityResolverStatic implements MapFunction<Event, UnifiedProfile> {
    @Override
    public UnifiedProfile map(Event event) {
        String id = getPrimaryIdentifier(event);
        UnifiedProfile profile = new UnifiedProfile();
        profile.setUnifiedUserId(UUID.nameUUIDFromBytes(id.getBytes()).toString());
        profile.addIdentifier(event);
        profile.updateMetadata(event);
        return profile;
    }

    public static String getPrimaryIdentifier(Event event) {
        if (event.getUserId() != null) return event.getUserId();
        // for web platform, capture cookie id
        if (event.getCookieId() != null) return event.getCookieId();
        // for other platforms, capture device id
        if (event.getDeviceId() != null) return event.getDeviceId();
        // if none is present capture the event to sideline topic
        // or return with UUID
        return UUID.randomUUID().toString();
    }
}
