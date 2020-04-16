package com.pvub.graphdemo;

import io.vertx.core.json.JsonObject;
import java.util.HashMap;

/**
 * A route between two airports
 * @author Udai
 */
public class Route extends Node {
    private final static String CREATE_RELATIONSHIP = "MATCH (src:Airport {code:$source}), (dst:Airport {code:$destination})\n"
                                              + "CREATE (src)-[r:Connects {airline:$airline,source:$source,destination:$destination}]->(dst)\n"
                                              + "RETURN r";
    private String m_source;
    private String m_destination;
    private String m_airline;
    private HashMap<String, Object> m_params = new HashMap<String, Object>();
    
    public Route(String source, String destination, String airline) {
        m_source = source;
        m_params.put("source", source);
        m_destination = destination;
        m_params.put("destination", destination);
        m_airline = airline;
        m_params.put("airline", airline);
    }
    
    @Override
    public String getCreateCypher() {
        return CREATE_RELATIONSHIP;
    }
    @Override
    public HashMap<String, Object> getCreateParams()
    {
        return m_params;
    }

    /**
     * @return the m_source
     */
    public String getSource() {
        return m_source;
    }

    /**
     * @return the m_destination
     */
    public String getDestination() {
        return m_destination;
    }

    /**
     * @return the m_airline
     */
    public String getAirline() {
        return m_airline;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(m_source).append("-(").append(m_airline).append(")->").append(m_destination);
        return sb.toString();
    }
    
    public JsonObject toJson() {
        JsonObject js = new JsonObject();
        js.put("origin", m_source);
        js.put("destination", m_destination);
        js.put("airline", m_airline);
        return js;
    }

}
