package com.pvub.graphdemo;

import io.vertx.core.json.JsonArray;
import java.util.ArrayList;
import java.util.List;

/**
 * Collection of routes
 * @author Udai
 */
public class AirPath {
    private List<Route> routes = new ArrayList<Route>();
    
    public AirPath() {}
    public void add(Route r) {
        routes.add(r);
    }
    public List<Route> getRoutes() {
        return routes;
    }
    public JsonArray toJson() {
        JsonArray arr = new JsonArray();
        routes.forEach(r -> { arr.add(r.toJson()); });
        return arr;
    }
}
