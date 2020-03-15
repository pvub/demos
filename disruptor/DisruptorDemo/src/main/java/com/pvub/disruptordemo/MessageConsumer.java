package com.pvub.disruptordemo;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event consumer
 * @author Udai
 */
public class MessageConsumer implements EventConsumer {
    private final Disruptor<MessageEvent> m_disruptor;
    private final Logger    m_logger;
    private AtomicLong      m_counter = new AtomicLong(0);
    private JsonObject      m_stats = new JsonObject();
    
    public MessageConsumer(final Disruptor<MessageEvent> disruptor)
    {
        m_disruptor = disruptor;
        m_logger = LoggerFactory.getLogger("CONSUMER");
    }
    
    @Override
    public void start() {
        m_logger.info("Starting consumer");
        m_disruptor.handleEventsWith(getEventHandler());
    }

    @Override
    public void stop() {
        m_logger.info("Starting consumer");
    }

    private EventHandler<MessageEvent> getEventHandler() {
        final EventHandler<MessageEvent> eventHandler = (event, sequence, endOfBatch) -> {
            consume(event, sequence);
        };
        return eventHandler;
    }
    
    private void consume(MessageEvent event, long sequence) {
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