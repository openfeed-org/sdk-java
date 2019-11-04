package org.openfeed.client.api;


public class OpenfeedEvent {
    public enum EventType {
        Connected, Disconnected, Login, Logout;
    };

    private EventType type;
    private String message;

    public OpenfeedEvent(EventType type, String message) {
        super();
        this.type = type;
        this.message = message;
    }

    public EventType getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }
}
