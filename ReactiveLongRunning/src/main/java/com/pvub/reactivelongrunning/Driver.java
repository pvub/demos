package com.pvub.reactivelongrunning;

import io.vertx.core.VertxOptions;
import io.vertx.rxjava.core.Vertx;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Driver {
    private Vertx   m_vertx = null;
    private Logger  m_logger;
    public static void main(String args[]) {
        Driver d = new Driver();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Running Shutdown Hook");
                d.stop();
            }
        });
        d.start();
    }

    public Driver() {
        m_logger = LoggerFactory.getLogger("DRIVER");
    }

    public void start() {
        m_logger.info("Starting Driver");
        m_vertx = Vertx.vertx(new VertxOptions().setBlockedThreadCheckInterval(1000));
        m_vertx.deployVerticle(new ApiVerticle());

    }

    public void stop() {
        m_logger.info("Stopping Driver");
        Set<String> ids = m_vertx.deploymentIDs();
        for (String id : ids) {
            m_vertx.undeploy(id, (result) -> {
                if (result.succeeded()) {

                } else {

                }
            });
        }
    }
}
