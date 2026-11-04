package com.vistay.notification.application;

/** Stable, redacted command failure used by GraphQL and provider boundaries. */
public class NotificationCommandException extends RuntimeException {
    private final String code;

    public NotificationCommandException(String code, String message) {
        super(message);
        this.code = code;
    }

    public String code() {
        return code;
    }
}
