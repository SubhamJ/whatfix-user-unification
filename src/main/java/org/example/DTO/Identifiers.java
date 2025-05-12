package org.example.DTO;

public class Identifiers {
    private String userId;
    private String cookieId;
    private String deviceId;

    public String getUserId () {
        return userId;
    }

    public void setUserId ( String userId ) {
        this.userId = userId;
    }

    public String getCookieId () {
        return cookieId;
    }

    public void setCookieId ( String cookieId ) {
        this.cookieId = cookieId;
    }

    public String getDeviceId () {
        return deviceId;
    }

    public void setDeviceId ( String deviceId ) {
        this.deviceId = deviceId;
    }
}
