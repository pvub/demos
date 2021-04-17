package com.pvub.parser;

import io.vertx.core.json.JsonObject;

import java.util.regex.Pattern;

public class LogEvent {
    // Match error, exception
    private static final Pattern pattern = Pattern.compile("\\be.*r\\b|\\be.*n\\b", Pattern.CASE_INSENSITIVE);
    private final String timestamp;
    private final long   timestampMS;
    private final String classname;
    private final String message;
    private final boolean error;

    public LogEvent(final String timestamp,
                    final long   timestampMS,
                    final String classname,
                    final String message) {
        this.timestamp      = timestamp;
        this.timestampMS    = timestampMS;
        this.classname      = classname;
        this.message        = message;
        this.error          = checkError();
    }

    public JsonObject toJson() {
        JsonObject obj = new JsonObject();
        obj.put("timestamp", timestamp);
        obj.put("className", classname);
        obj.put("message", message);
        return obj;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getClassname() {
        return classname;
    }

    public String getMessage() {
        return message;
    }

    public long getTimestampMS() {
        return timestampMS;
    }

    public boolean isError() {
        return error;
    }

    private boolean checkError() {
        return pattern.matcher(this.message).find();
    }
}
