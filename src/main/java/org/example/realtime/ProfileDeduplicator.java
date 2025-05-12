package org.example.realtime;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.example.DTO.UnifiedProfile;

// profile deduplication can be done using state store like RocksDB
public class ProfileDeduplicator extends RichMapFunction<UnifiedProfile, UnifiedProfile> {

    private transient MapState<String, Boolean> seenProfiles;

    @Override
    public void open(Configuration parameters) {
        seenProfiles = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("seen-profiles", String.class, Boolean.class));
    }

    @Override
    public UnifiedProfile map(UnifiedProfile profile) throws Exception {
        String id = profile.getUnifiedUserId();

        if (seenProfiles.contains(id)) {
            return null; // Skip duplicate
        } else {
            seenProfiles.put(id, true);
            return profile;
        }
    }
}

