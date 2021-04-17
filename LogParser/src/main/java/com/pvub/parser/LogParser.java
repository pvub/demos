package com.pvub.parser;

import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import io.vertx.core.json.JsonObject;
import io.reactivex.disposables.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LogParser {
    private final DateTimeFormatter     tsFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss")
                                                                        .withZone(ZoneId.of("UTC"));;

    private Logger                      logger;
    private BoundedBucket               bucket = new BoundedBucket();
    private PublishSubject<LogEvent>    eventSubject = PublishSubject.create();
    private long                        errorCount = 0;
    private FileWriter                  writer = null;
    private CountDownLatch              latch = null;
    private Disposable                  writerSubscription = null;

    public LogParser() {
        logger = LoggerFactory.getLogger("PARSER");
    }

    public void parse(String applicationLogFile, String errorFile) {
        parse(applicationLogFile, errorFile, 0);
    }

    public void parse(String applicationLogFile, String errorFile, long startingRow) {
        logger.info("Parsing file {}, collecting errors at {}. Starting at row={}", applicationLogFile, errorFile, startingRow);
        Path inputFile   = Paths.get(applicationLogFile);
        latch = new CountDownLatch(1);
        record(errorFile);
        read(inputFile.toFile(), startingRow);

        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                logger.error("Operation taking longer then 30 seconds");
            }
        } catch (InterruptedException e) {
            logger.error("Error waiting", e);
        }
    }

    private void record(String errorFile) {
        try {
            writer = new FileWriter(errorFile);
            logger.info("Writer created");
            writer.write(getBeginString());
            writerSubscription =
                eventSubject.subscribe(
                        event -> {
                            recordEvent(event);
                            if (event.isError()) {
                                logger.info("Error @ {} - {}", event.getTimestampMS(), event.getMessage());
                                ++errorCount;
                                JsonObject json = event.toJson();
                                json.put("previousLogs", bucket.getJson());
                                if (errorCount > 1) {
                                    writer.write(',');
                                }
                                writer.write(json.encode());
                            }
                        },
                        error -> {
                            closeWriter();
                            latch.countDown();
                        },
                        () -> {
                            closeWriter();
                            latch.countDown();
                        }
                );
        } catch (IOException e) {
            logger.error("Error writing file", e);
        }
    }

    private void closeWriter() {
        if (writerSubscription != null) {
            writerSubscription.dispose();
            writerSubscription = null;
        }
        if (writer != null) {
            try {
                writer.write(getEndString(errorCount));
                writer.flush();
                writer.close();
            } catch (IOException e) {
                logger.error("Error closing file", e);
            }
        }
    }

    private void recordEvent(LogEvent event) {
        bucket.add(event);
    }

    private void read(File inputFile, long startingRow) {
        scan(inputFile, startingRow)
                .subscribe(
                        line -> {
                            LogEvent event = getLogEvent(line);
                            if (event != null) {
                                eventSubject.onNext(event);
                            }
                        },
                        error -> {
                            eventSubject.onError(error);
                        },
                        () -> {
                            eventSubject.onComplete();
                        }
                );
    }

    private Observable<String> scan(File inputFile, long startingRow) {
        return Observable.create(subscriber -> {
            FileInputStream inputStream = null;
            Scanner sc = null;
            long currentRow = 0;
            try {
                inputStream = new FileInputStream(inputFile);
                sc = new Scanner(inputStream, "UTF-8");
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    if (currentRow++ >= startingRow) {
                        subscriber.onNext(line);
                    }
                }
                // note that Scanner suppresses exceptions
                if (sc.ioException() != null) {
                    subscriber.onError(sc.ioException());
                }
                subscriber.onComplete();
            } catch(IOException e) {
                subscriber.onError(e);
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
                if (sc != null) {
                    sc.close();
                }
            }
        });
    }

    private LogEvent getLogEvent(String line) {
        if (line.isEmpty() || line.length() < 21) {
            return null;
        }

        LogEvent logEvent = null;
        // Parse timestamp. 2020-04-01 10:09:22
        String timestamp = line.substring(0, 19);
        long   timestampMS = getMilli(timestamp);
        // Get classname and message
        String remaining = line.substring(20);
        int hyphenIndex   = remaining.indexOf('-');
        if (hyphenIndex > -1) {
            String classname = remaining.substring(0, hyphenIndex - 1).trim();
            String message   = remaining.substring(hyphenIndex + 1).trim();
            logEvent = new LogEvent(timestamp, timestampMS, classname, message);
        }
        return logEvent;
    }

    private long getMilli(final String timestampStr) {
        ZonedDateTime tsDate = ZonedDateTime.from(tsFormatter.parse(timestampStr));
        return tsDate.toInstant().toEpochMilli();
    }

    private String getBeginString() {
        return "{\"errors\": [";
    }

    private String getEndString(long errorCount) {
        return "], \"errorCount\": " + errorCount + "}";
    }
}
