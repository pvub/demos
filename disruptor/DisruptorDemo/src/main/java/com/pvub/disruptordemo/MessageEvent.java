package com.pvub.disruptordemo;

import com.lmax.disruptor.EventFactory;

/**
 * Event that is generated
 * @author Udai
 */
public final class MessageEvent {
    private int value;
    public int getValue() {
        return value;
    }
    public void setValue(int value) {
        this.value = value;
    }

    public final static EventFactory<MessageEvent> EVENT_FACTORY = () -> new MessageEvent();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("val=").append(value);
        return sb.toString();
    }
}