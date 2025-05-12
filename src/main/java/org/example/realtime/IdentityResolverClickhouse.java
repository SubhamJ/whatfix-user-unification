package org.example.realtime;

import org.example.DTO.Event;
import org.example.DTO.UnifiedProfile;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.UUID;

// Identify resolver using clickhouse and redis
public class IdentityResolverClickhouse implements MapFunction<Event, UnifiedProfile> {

    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://localhost:8123/user_unif";
    private static final Logger LOGGER = LoggerFactory.getLogger(IdentityResolverClickhouse.class);

    @Override
    public UnifiedProfile map(Event event) throws Exception {
        String resolvedUnifiedId = findExistingUnifiedUserId(event);
        if (resolvedUnifiedId == null) {
            resolvedUnifiedId = UUID.nameUUIDFromBytes(getLookupKey(event).getBytes()).toString();
        }

        UnifiedProfile profile = new UnifiedProfile();
        profile.setUnifiedUserId(resolvedUnifiedId);
        profile.addIdentifier(event);
        profile.updateMetadata(event);

        return profile;
    }

    private String findExistingUnifiedUserId(Event event) {
        try (Connection conn = DriverManager.getConnection(CLICKHOUSE_URL)) {
            LOGGER.info("Event for Identity Resolution: {}", event.toString());
            // user_id lookup
            if (event.getUserId() != null) {
                LOGGER.info("LookUp for UserId: {}", event.getUserId());
                String uid = lookup(conn, "user_ids", event.getUserId());
                LOGGER.info("UserID: {}, Unified ID:{}", event.getUserId(), uid);
                if (uid != null) return uid;
            }

            // cookie_id lookup
            if (event.getCookieId() != null) {
                LOGGER.info("LookUp for CookieId: {}", event.getCookieId());
                String uid = lookup(conn, "cookie_ids", event.getCookieId());
                LOGGER.info("CookieId: {}, Unified ID:{}", event.getCookieId(), uid);
                if (uid != null) return uid;
            }

            // device_id lookup
            if (event.getDeviceId() != null) {
                LOGGER.info("LookUp for DeviceId: {}", event.getDeviceId());
                String uid = lookup(conn, "device_ids", event.getDeviceId());
                LOGGER.info("DeviceId: {}, Unified ID:{}", event.getDeviceId(), uid);
                if (uid != null) return uid;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        LOGGER.info("No unified ID found!");
        return null;
    }

    private String lookup(Connection conn, String columnName, String identifierValue) throws SQLException {
        String sql = String.format(
                "SELECT unified_user_id FROM unified_profiles WHERE %s=? LIMIT 1", columnName);
        LOGGER.info("SQL: {}, Parameter: {}", sql, identifierValue);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, identifierValue);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString("unified_user_id");
            }
        }
        return null;
    }

    public static String getLookupKey(Event event) {
        if (event.getUserId() != null) return event.getUserId();
        if (event.getCookieId() != null) return event.getCookieId();
        if (event.getDeviceId() != null) return event.getDeviceId();
        return UUID.randomUUID().toString();
    }
}

