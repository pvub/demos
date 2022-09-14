package com.pvub.reactivelongrunning;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

// Worker verticle to asynchronously process requests
public class ApiWorkerVerticle extends AbstractVerticle {
    private Logger          m_logger;
    private Integer         m_max_delay_milliseconds;
    private JsonObject      m_config = null;
    private ExecutorService m_worker_executor = null;
    private Scheduler       m_scheduler;
    private AtomicLong      m_latency = new AtomicLong(0);
    private MessageConsumer<String> worker_consumer = null;
    private ConcurrentHashMap<String, JsonObject> activeTransactions = new ConcurrentHashMap<>();

    public ApiWorkerVerticle() {
        super();
    }

    @Override
    public void start() throws Exception {
        m_logger = LoggerFactory.getLogger("WORKER");

        String path_to_config = System.getProperty("reactiveapi.config", "conf/config.json");
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setFormat("json")
                .setConfig(new JsonObject().put("path", path_to_config));

        ConfigRetriever retriever = ConfigRetriever.create(vertx,
                new ConfigRetrieverOptions().addStore(fileStore));
        retriever.getConfig(
                config -> {
                    m_logger.info("config retrieved");
                    if (config.failed()) {
                        m_logger.info("No config");
                    } else {
                        m_logger.info("Got config");
                        startup(config.result());
                    }
                }
        );
    }

    private void processConfig(JsonObject config) {
        m_config = config;
    }

    private void startup(JsonObject config) {
        processConfig(config);
        m_max_delay_milliseconds = m_config.getInteger("max-delay-milliseconds", 1000);
        Integer worker_pool_size = m_config.getInteger("worker-pool-size", Runtime.getRuntime().availableProcessors() * 2);
        m_logger.info("max_delay_milliseconds={} worker_pool_size={}", m_max_delay_milliseconds, worker_pool_size);
        m_worker_executor = Executors.newFixedThreadPool(worker_pool_size);
        m_scheduler = Schedulers.from(m_worker_executor);
        worker_consumer = vertx.eventBus().consumer("WORKER");
        worker_consumer.handler(m -> {
            handleRequest(m);
        });
    }

    private void handleRequest(Message<String> m) {
        JsonObject requestObject = new JsonObject(m.body());
        final String tid = requestObject.getString("tid");
        activeTransactions.put(tid, requestObject);
        requestObject.put("status", TransactionStatus.PENDING.value());
        requestObject.put("type", "transaction-status");
        m.reply(requestObject.encode());
        handleTransaction(tid);
    }

    private void handleTransaction(final String tid) {
        vertx.setTimer(5000, x -> {
            updateTransaction(tid);
        });
    }

    private void updateTransaction(final String tid) {
        JsonObject requestObject = activeTransactions.get(tid);
        if (requestObject != null) {
            TransactionStatus status = TransactionStatus.valueOf(requestObject.getString("status"));
            if (status.ordinal() < TransactionStatus.COMPLETE.ordinal()) {
                TransactionStatus nextStatus = TransactionStatus.values()[status.ordinal() + 1];
                requestObject.put("status", nextStatus.value());
                requestObject.put("type", "transaction-status");
                activeTransactions.put(tid, requestObject);
                publishTransaction(requestObject);
                if (nextStatus.ordinal() < TransactionStatus.COMPLETE.ordinal()) {
                    handleTransaction(tid);
                } else {
                    activeTransactions.remove(tid);
                }
            }
        }
    }

    private void publishTransaction(final JsonObject obj) {
        vertx.eventBus().publisher(obj.getString("customer")).write(obj.encode());
    }

    @Override
    public void stop() throws Exception {
        m_logger.info("Stopping ApiWorkerVerticle");
        m_worker_executor.shutdown();
    }
}