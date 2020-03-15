package com.pvub.disruptordemo;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.BackpressureOverflow;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

/**
 * Event consumer
 * @author Udai
 */
public class RxMessageConsumer implements EventConsumer {
    private final PublishSubject<MessageEvent> m_subject;
    private final Logger    m_logger;
    private ExecutorService m_executor = null;
    private Scheduler       m_scheduler;
    private AtomicLong      m_counter = new AtomicLong(0);
    private JsonObject      m_stats = new JsonObject();
    private Subscription    m_subscription = null;
    
    public RxMessageConsumer(JsonObject config, final PublishSubject<MessageEvent> subject)
    {
        m_subject = subject;
        m_logger = LoggerFactory.getLogger("RXCONSUMER");
        Integer worker_pool_size = config.getInteger("worker-pool-size", 1);
        m_executor = Executors.newFixedThreadPool(worker_pool_size);
        m_scheduler = Schedulers.from(m_executor);
    }
    
    @Override
    public void start() {
        m_logger.info("Starting consumer");
        m_subscription = m_subject
                        .onBackpressureBuffer(100000000
                                             , () -> { 
                                                 m_logger.info("Producer backpressure"); 
                                             }
                                             , BackpressureOverflow.ON_OVERFLOW_DROP_LATEST)
                        .observeOn(m_scheduler)
                        .subscribe(e -> {
                               consume(e);
                           }, 
                           error -> {
                               m_logger.error("Error consumer from subject", error);
                           }, 
                           () -> {
                           });
    }

    @Override
    public void stop() {
        m_logger.info("Stopping consumer");
        m_subscription.unsubscribe();
    }

    private void consume(MessageEvent event) {
        m_counter.incrementAndGet();
    }
    
    public String getStats() {
        m_stats.put("count", m_counter.getAndSet(0));
        return Json.encode(m_stats);
    }
    public long getCount() {
        return m_counter.getAndSet(0);
    }
}