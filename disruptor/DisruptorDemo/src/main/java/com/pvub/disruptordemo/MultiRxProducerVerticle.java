package com.pvub.disruptordemo;

import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.config.ConfigRetriever;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.MessageProducer;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

/**
 * Verticle to handle API requests
 * @author Udai
 */
public class MultiRxProducerVerticle extends AbstractVerticle {
    static final int WORKER_POOL_SIZE = 20;

    private final Logger    m_logger;
    private JsonObject      m_config = null;
    private ExecutorService m_worker_executor = null;
    private Scheduler       m_scheduler;
    
    private ArrayList<RxEventProducer> m_producers = new ArrayList<RxEventProducer>();
    private RxMessageConsumer m_consumer = null;
    private PublishSubject<MessageEvent> m_subject = null;
    private Subscription m_subscription;
    private Integer m_producer_count = 2;
    private long m_total = 0;
    
    private MessageProducer<String> eb_producer = null;

    public MultiRxProducerVerticle() {
        super();
        m_logger = LoggerFactory.getLogger("RXMULTI");
    }
    @Override
    public void start() throws Exception {
        m_logger.info("Starting MultiRxProducerVerticle");

        ConfigRetriever retriever = ConfigRetriever.create(vertx);
        retriever.getConfig(
            config -> {
                m_logger.info("config retrieved");
                if (config.failed()) {
                    m_logger.info("No config");
                } else {
                    m_logger.info("Got config");
                    m_config = config.result();
                    m_producer_count = m_config.getInteger("producer-count", 2);
                    m_worker_executor = Executors.newSingleThreadExecutor();
                    m_scheduler = Schedulers.from(m_worker_executor);
                    startup();
                }
            }
        );
    }
    
    @Override
    public void stop() throws Exception {
        m_logger.info("Stopping MultiRxProducerVerticle");
        m_subscription.unsubscribe();
        Observable.from(m_producers)
                .subscribe(producer -> {
                    producer.stop();
                });
        m_consumer.stop();
        m_producers.clear();
    }
    
    private void startup() {
        m_subject = PublishSubject.create();
        Observable.range(1, m_producer_count)
                .subscribe(idx -> {
                    m_producers.add(new RxEventProducer(m_config, m_subject));
                });
        m_consumer = new RxMessageConsumer(m_config, m_subject);
        m_consumer.start();
        Observable.from(m_producers)
                .subscribe(producer -> {
                    producer.start();
                });
        eb_producer = vertx.eventBus().publisher("app.to.console");

        m_subscription = Observable.interval(1, TimeUnit.SECONDS)
                                .observeOn(m_scheduler)
                                .subscribe(delay -> {
                                            Observable.from(m_producers)
                                                    .subscribe(producer -> {
                                                        m_total += producer.getCount();
                                                    });

                                            long produced = m_total;
                                            long consumed = m_consumer.getCount();
                                            m_logger.info("Multi Rx produced={} consumed={}", produced, consumed);
                                            JsonObject obj = new JsonObject();
                                            obj.put("design", "m2srx");
                                            obj.put("produced", produced);
                                            obj.put("consumed", consumed);
                                            eb_producer.send(obj.encode());

                                            m_total = 0;
                                        }, 
                                        error -> {}, 
                                        () -> {});
    }

}