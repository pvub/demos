package com.pvub.graphdemo;

import java.util.HashMap;

/**
 * Node abstract class
 * @author Udai
 */
public abstract class Node {
    public abstract String getCreateCypher();
    public abstract HashMap<String, Object> getCreateParams();
}
