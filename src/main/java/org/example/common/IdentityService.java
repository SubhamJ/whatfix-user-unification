package org.example.common;

import org.example.DTO.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.util.UUID;

public class IdentityService {
    private static final Logger LOGGER = LoggerFactory.getLogger(IdentityService.class);
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://localhost:8123/user_unif";

    private final Jedis redis;

    public IdentityService(Jedis redis) {
        this.redis = redis;
    }

    public String resolveUnifiedUserId(Event event) {
        String id = lookupRedis(event);
        if (id != null) return id;

        id = lookupClickHouse(event);
        if (id != null) return id;

        return UUID.nameUUIDFromBytes(getLookupKey(event).getBytes()).toString();
    }

    public void cacheIdentifiers(Event event, String unifiedId) {
        if (event.getUserId() != null)
            redis.setnx("user_id:" + event.getUserId(), unifiedId);
        if (event.getCookieId() != null)
            redis.setnx("cookie_id:" + event.getCookieId(), unifiedId);
        if (event.getDeviceId() != null)
            redis.setnx("device_id:" + event.getDeviceId(), unifiedId);
    }

    private String lookupRedis(Event event) {
        if (event.getUserId() != null) {
            String id = redis.get("user_id:" + event.getUserId());
            LOGGER.info("Redis user_id: {} → {}", event.getUserId(), id);
            if (id != null) return id;
        }
        if (event.getCookieId() != null) {
            String id = redis.get("cookie_id:" + event.getCookieId());
            LOGGER.info("Redis cookie_id: {} → {}", event.getCookieId(), id);
            if (id != null) return id;
        }
        if (event.getDeviceId() != null) {
            String id = redis.get("device_id:" + event.getDeviceId());
            LOGGER.info("Redis device_id: {} → {}", event.getDeviceId(), id);
            if (id != null) return id;
        }
        return null;
    }

    private String lookupClickHouse(Event event) {
        try (Connection conn = DriverManager.getConnection(CLICKHOUSE_URL)) {
            if (event.getUserId() != null) {
                String uid = lookup(conn, "user_ids", event.getUserId());
                if (uid != null) return uid;
            }
            if (event.getCookieId() != null) {
                String uid = lookup(conn, "cookie_ids", event.getCookieId());
                if (uid != null) return uid;
            }
            if (event.getDeviceId() != null) {
                String uid = lookup(conn, "device_ids", event.getDeviceId());
                if (uid != null) return uid;
            }
        } catch (SQLException e) {
            LOGGER.error("ClickHouse lookup error: {}", e.getMessage(), e);
        }
        return null;
    }

    private String lookup(Connection conn, String columnName, String identifier) throws SQLException {
        String sql = String.format(
                "SELECT unified_user_id FROM unified_profiles WHERE %s=? LIMIT 1", columnName);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, identifier);
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

