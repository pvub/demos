package com.pvub.parser;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class BoundedBucket {
    private final   long                FIVE_SECONDS_IN_MILLISECONDS = 5 * 1000; // in milliseconds
    private final   ArrayList<LogEvent> bucket;

    public BoundedBucket() {
        this.bucket         = new ArrayList<>();
    }

    public void add(LogEvent event) {
        bucket.add(event);
        if (bucket.size() < 2) {
            return;
        }
        while(isOld(bucket.get(0), event)) {
            bucket.remove(0);
        }
    }

    public JsonArray getJson() {
        JsonArray logs = new JsonArray();
        for (int index = 0; index < bucket.size() - 1; ++index) {
            logs.add(bucket.get(index).toJson());
        }
        return logs;
    }

    public void reset() {
        this.bucket.clear();
    }

    private boolean isOld(LogEvent pastEvent, LogEvent presentEvent) {
        return (presentEvent.getTimestampMS() - pastEvent.getTimestampMS()) > FIVE_SECONDS_IN_MILLISECONDS;
    }
}


