package com.pvub.disruptordemo;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.config.ConfigRetriever;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.http.HttpServerResponse;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.handler.CorsHandler;
import io.vertx.rxjava.ext.web.handler.StaticHandler;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;
import io.vertx.rxjava.core.eventbus.MessageProducer;

/**
 * Verticle to handle API requests
 * @author Udai
 */
public class MultiProducerVerticle extends AbstractVerticle {
    static final int WORKER_POOL_SIZE = 20;

    private final Logger    m_logger;
    private JsonObject      m_config = null;
    private ExecutorService m_worker_executor = null;
    private Scheduler       m_scheduler;
    
    private ArrayList<EventProducer> m_producers = new ArrayList<EventProducer>();
    private MessageConsumer m_consumer = null;
    private Disruptor<MessageEvent> m_disruptor = null;
    private Subscription m_subscription;
    private Integer m_producer_count = 2;
    private long m_total = 0;
    private MessageProducer<String>   m_worker_producer = null;
    private Subscription              m_worker_timer = null;
    private String                    m_tag;
    
    public MultiProducerVerticle() {
        super();
        m_logger = LoggerFactory.getLogger("MULTI");
    }
    @Override
    public void start() throws Exception {
        m_logger.info("Starting MultiProducerVerticle");

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
        m_logger.info("Stopping MultiProducerVerticle");
        m_subscription.unsubscribe();
        Observable.from(m_producers)
                .subscribe(producer -> {
                    producer.stop();
                });
        m_disruptor.halt();
        m_disruptor.shutdown();
        m_consumer.stop();
    }
    
    private void startup() {
        int buffer_size = m_config.getInteger("ring-buffer-size-power-of-2", 12);
        m_logger.info("Starting disruptor with buffer size {}", Math.pow(2, buffer_size));
        m_disruptor = new Disruptor<>(MessageEvent.EVENT_FACTORY
                                     , (int)Math.pow(2, buffer_size)
                                     , DaemonThreadFactory.INSTANCE
                                     , ProducerType.MULTI
                                     , new BusySpinWaitStrategy());
        Observable.range(1, m_producer_count)
                .subscribe(idx -> {
                    m_producers.add(new EventProducer(m_config, m_disruptor));
                });
        m_consumer = new MessageConsumer(m_disruptor);
        m_consumer.start();
        m_disruptor.start();
        Observable.from(m_producers)
                .subscribe(producer -> {
                    producer.start();
                });

        m_worker_producer = vertx.eventBus().publisher("app.to.console");
        m_subscription = Observable.interval(1, TimeUnit.SECONDS)
                                .observeOn(m_scheduler)
                                .subscribe(delay -> {
                                            Observable.from(m_producers)
                                                    .subscribe(producer -> {
                                                        m_total += producer.getCount();
                                                    });
                                            long produced = m_total;
                                            long consumed = m_consumer.getCount();
                                            m_logger.info("Multi Consumer={} producer={}", consumed, produced);
                                            JsonObject obj = new JsonObject();
                                            obj.put("design", "m2sdrp");
                                            obj.put("produced", produced);
                                            obj.put("consumed", consumed);
                                            m_worker_producer.send(obj.encode());
                                            m_total = 0;
                                       }, 
                                        error -> {}, 
                                        () -> {});
    }

}