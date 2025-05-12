package org.example.common;

import org.example.DTO.UnifiedProfile;
import org.example.DTO.Identifiers;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;

public class ClickHouseSink extends RichSinkFunction<UnifiedProfile> {
    private transient Connection connection;
    private transient PreparedStatement insertStmt;
    private transient PreparedStatement existsStmt;


    private static final String INSERT_SQL = "INSERT INTO unified_profiles " +
            "(unified_user_id, user_ids, cookie_ids, device_ids, platforms) " +
            "VALUES (?, ?, ?, ?, ?)";

    private static final String CHECK_EXIST_SQL = "SELECT count() FROM unified_profiles " +
            "WHERE user_ids = ? AND cookie_ids = ? AND device_ids = ? AND platforms = ? LIMIT 1";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        String url = "jdbc:clickhouse://localhost:8123/user_unif";
        connection = DriverManager.getConnection(url);
        insertStmt = connection.prepareStatement(INSERT_SQL);
        existsStmt = connection.prepareStatement(CHECK_EXIST_SQL);
    }

    @Override
    public void invoke(UnifiedProfile profile, Context context) throws Exception {
        Identifiers id = profile.getIdentifiers();

        String userIds = id.getUserId();
        String cookieIds = id.getCookieId();
        String deviceIds = id.getDeviceId();
        String platforms = profile.getPlatform();

        // check if user profile exists - deduplication
        existsStmt.setString(1, userIds);
        existsStmt.setString(2, cookieIds);
        existsStmt.setString(3, deviceIds);
        existsStmt.setString(4, platforms);

        ResultSet rs = existsStmt.executeQuery();
        rs.next();
        long count = rs.getLong(1);

        if (count == 0) {
            insertStmt.setString(1, profile.getUnifiedUserId());
            insertStmt.setString(2, id.getUserId() == null ? "" : id.getUserId());
            insertStmt.setString(3, id.getCookieId() == null ? "" : id.getCookieId());
            insertStmt.setString(4, id.getDeviceId() == null ? "" : id.getDeviceId());
            insertStmt.setString(5, profile.getPlatform());
            insertStmt.executeUpdate();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (insertStmt != null) insertStmt.close();
        if (existsStmt != null) existsStmt.close();
        if (connection != null) connection.close();
    }
}

