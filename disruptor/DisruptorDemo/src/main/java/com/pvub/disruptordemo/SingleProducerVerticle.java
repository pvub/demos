package com.pvub.disruptordemo;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.config.ConfigRetriever;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.eventbus.MessageProducer;
import io.vertx.rxjava.core.http.HttpServerResponse;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.handler.CorsHandler;
import io.vertx.rxjava.ext.web.handler.StaticHandler;
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

/**
 * Verticle to handle API requests
 * @author Udai
 */
public class SingleProducerVerticle extends AbstractVerticle {
    static final int WORKER_POOL_SIZE = 20;

    private final Logger    m_logger;
    private Integer         m_max_delay_milliseconds;
    private JsonObject      m_config = null;
    private ExecutorService m_worker_executor = null;
    private Scheduler       m_scheduler;
    
    private EventProducer m_producer = null;
    private MessageConsumer m_consumer = null;
    private Disruptor<MessageEvent> m_disruptor = null;
    private Subscription m_subscription;
    private MessageProducer<String> eb_producer = null;
    
    public SingleProducerVerticle() {
        super();
        m_logger = LoggerFactory.getLogger("SINGLE");
    }
    @Override
    public void start() throws Exception {
        m_logger.info("Starting SingleProducerVerticle");

        ConfigRetriever retriever = ConfigRetriever.create(vertx);
        retriever.getConfig(
            config -> {
                m_logger.info("config retrieved");
                if (config.failed()) {
                    m_logger.info("No config");
                } else {
                    m_logger.info("Got config");
                    m_config = config.result();
                    m_max_delay_milliseconds = m_config.getInteger("max-delay-milliseconds", 1000);
                    Integer worker_pool_size = m_config.getInteger("worker-pool-size", Runtime.getRuntime().availableProcessors() * 2);
                    m_logger.info("max_delay_milliseconds={} worker_pool_size={}", m_max_delay_milliseconds, worker_pool_size);
                    m_worker_executor = Executors.newFixedThreadPool(worker_pool_size);
                    m_scheduler = Schedulers.from(m_worker_executor);
                    startup();
                }
            }
        );
    }
    
    @Override
    public void stop() throws Exception {
        m_logger.info("Stopping SingleProducerVerticle");
        m_subscription.unsubscribe();
        m_producer.stop();
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
                                     , ProducerType.SINGLE
                                     , new BusySpinWaitStrategy());
        m_producer = new EventProducer(m_config, m_disruptor);
        m_consumer = new MessageConsumer(m_disruptor);
        m_consumer.start();
        m_disruptor.start();
        m_producer.start();
        eb_producer = vertx.eventBus().publisher("app.to.console");
        
        m_subscription = Observable.interval(1, TimeUnit.SECONDS)
                                .observeOn(m_scheduler)
                                .subscribe(delay -> {
                                            long produced = m_producer.getCount();
                                            long consumed = m_consumer.getCount();
                                            m_logger.info("Single produced={} consumed={}", produced, consumed);
                                            JsonObject obj = new JsonObject();
                                            obj.put("design", "s2sdrp");
                                            obj.put("produced", produced);
                                            obj.put("consumed", consumed);
                                            eb_producer.send(obj.encode());
                                        }, 
                                        error -> {}, 
                                        () -> {});
    }

}