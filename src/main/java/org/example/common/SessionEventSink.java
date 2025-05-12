package org.example.common;

import org.example.DTO.EnrichedEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SessionEventSink extends RichSinkFunction<EnrichedEvent> {

    private transient Connection connection;
    private transient PreparedStatement stmt;

    private static final String SQL = "INSERT INTO session_events " +
            "(unified_user_id, session_id, event_type, event_timestamp, user_id, cookie_id, device_id, platform) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        String url = "jdbc:clickhouse://localhost:8123/user_unif";
        connection = DriverManager.getConnection(url);
        stmt = connection.prepareStatement(SQL);
    }

    @Override
    public void invoke(EnrichedEvent e, Context context) throws Exception {
        stmt.setString(1, e.getUnifiedUserId());
        stmt.setString(2, e.getSessionId());
        stmt.setString(3, e.getEventType());
        stmt.setLong(4, e.getEventTimestamp());
        stmt.setString(5, e.getUserId() == null ? "" : e.getUserId());
        stmt.setString(6, e.getCookieId() == null ? "" : e.getCookieId());
        stmt.setString(7, e.getDeviceId() == null ? "" : e.getDeviceId());
        stmt.setString(8, e.getPlatform() == null ? "" : e.getPlatform());
        stmt.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (stmt != null) stmt.close();
        if (connection != null) connection.close();
    }
}

