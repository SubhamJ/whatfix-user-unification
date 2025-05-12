package org.example.DTO;

public class EnrichedEvent extends Event {
    private String unifiedUserId;
    private String sessionId;
    private long eventTimestamp;

    public String getUnifiedUserId() { return unifiedUserId; }
    public void setUnifiedUserId(String unifiedUserId) { this.unifiedUserId = unifiedUserId; }

    public String getSessionId() { return sessionId; }
    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public long getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(long eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    @Override
    public String toString () {
        return "EnrichedEvent{" +
                "unifiedUserId='" + unifiedUserId + '\'' +
                ", sessionId='" + sessionId + '\'' +
                ", eventTimestamp=" + eventTimestamp +
                '}';
    }
}

