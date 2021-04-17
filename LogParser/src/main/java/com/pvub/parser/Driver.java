package com.pvub.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Driver {
    private Logger logger;

    public static void main(String args[]) {
        final Driver d = new Driver();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Running Shutdown Hook");
                d.stop();
            }
        });
        if (args.length >= 2) {
            String input = args[0];
            String output = args[1];
            long   startingRow = 0;
            if (args.length == 3) {
                startingRow = Long.parseLong(args[2]);
            }
            d.start(input, output, startingRow);
        } else {
            System.out.println("Usage java -jar <> <input> <output>");
        }
    }

    public Driver() {
        logger = LoggerFactory.getLogger("DRIVER");
    }

    public void start(final String inputFile, final String outputFile, long startingRow) {
        logger.info("Starting Driver");
        LogParser parser = new LogParser();
        parser.parse(inputFile, outputFile, startingRow);
    }

    public void stop() {
        logger.info("Stopping Driver");
    }
}
