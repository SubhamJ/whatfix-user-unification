package org.example.DTO;

public class UnifiedProfile {
    private String unifiedUserId;
    private final Identifiers identifiers = new Identifiers();
    private String platform;

    public String getUnifiedUserId() { return unifiedUserId; }
    public void setUnifiedUserId(String id) { this.unifiedUserId = id; }
    public Identifiers getIdentifiers() { return identifiers; }
    public String getPlatform() { return platform; }

    public void addIdentifier(Event e) {
        if (e.getUserId() != null) identifiers.setUserId(e.getUserId());
        if (e.getCookieId() != null) identifiers.setCookieId(e.getCookieId());
        if (e.getDeviceId() != null) identifiers.setDeviceId(e.getDeviceId());
    }

    public void updateMetadata(Event e) {
        if (e.getPlatform() != null) platform = e.getPlatform();
    }
}
