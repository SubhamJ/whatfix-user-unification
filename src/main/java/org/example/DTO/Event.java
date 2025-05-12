package org.example.DTO;

public class Event {
    private String userId;
    private String cookieId;
    private String deviceId;
    private String platform;
    private String timestamp;
    private String eventType;

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getCookieId() { return cookieId; }
    public void setCookieId(String cookieId) { this.cookieId = cookieId; }
    public String getDeviceId() { return deviceId; }
    public void setDeviceId(String deviceId) { this.deviceId = deviceId; }
    public String getPlatform() { return platform; }
    public void setPlatform(String platform) { this.platform = platform; }
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    @Override
    public String toString () {
        return "Event{" +
                "userId='" + userId + '\'' +
                ", cookieId='" + cookieId + '\'' +
                ", deviceId='" + deviceId + '\'' +
                ", platform='" + platform + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", eventType='" + eventType + '\'' +
                '}';
    }
}
