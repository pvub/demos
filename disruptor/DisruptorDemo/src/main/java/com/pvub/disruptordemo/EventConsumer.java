package com.pvub.disruptordemo;

import com.lmax.disruptor.EventHandler;

/**
 * Consumer that consumes event from ring buffer. 
 * @author Udai
 */
public interface EventConsumer {
    public void start();
    public void stop();
}