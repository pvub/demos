package com.pvub.graphdemo;

import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Airport POJO
 * @author Udai
 */
public class Airport extends Node {
    
    private static String CREATE = "CREATE (a:Airport {name: $name, city: $city, country: $country, code: $code"
                                 + ", latitude: $latitude, longitude: $longitude, altitude: $altitude, timezone: $timezone})\n"
                                 + "RETURN a";
    
    private final Logger    m_logger;
    private String      m_name;
    private String      m_city;
    private String      m_country;
    private String      m_code;
    private Float       m_latitude;
    private Float       m_longitude;
    private Integer     m_altitude;
    private String      m_timezone;
    private HashMap<String, Object> m_params = new HashMap<String, Object>();
    
    public Airport(String name, 
                   String city, 
                   String country, 
                   String code, 
                   Float latitude, 
                   Float longitude, 
                   Integer altitude, 
                   String timezone) 
    {
        m_logger = LoggerFactory.getLogger("AIRPORT");
        m_name = name;
        m_params.put("name", name);
        m_city = city;
        m_params.put("city", city);
        m_country = country;
        m_params.put("country", country);
        m_code = code;
        m_params.put("code", code);
        m_latitude = latitude;
        m_params.put("latitude", latitude);
        m_longitude = longitude;
        m_params.put("longitude", longitude);
        m_altitude = altitude;
        m_params.put("altitude", altitude);
        m_timezone = timezone;
        m_params.put("timezone", timezone);
    }
    public Airport(Value node) {
        m_logger = LoggerFactory.getLogger("AIRPORT");
        m_name = node.get("name").asString();
        m_city = node.get("city").asString();
        m_country = node.get("country").asString();
        m_code = node.get("code").asString();
        m_logger.info("Airport {} {} {} {}", m_name, m_city, m_country, m_code);
        m_latitude = node.get("latitude").asFloat();
        m_longitude = node.get("longitude").asFloat();
        m_altitude = node.get("altitude").asInt();
        m_timezone = node.get("timezone").asString();
    }
    
    @Override
    public String getCreateCypher() {
        return CREATE;
    }
    @Override
    public HashMap<String, Object> getCreateParams()
    {
        return m_params;
    }
    public JsonObject getJson() {
        JsonObject obj = new JsonObject();
        obj.put("name", m_name);
        obj.put("city", m_city);
        obj.put("country", m_country);
        obj.put("code", m_code);
        obj.put("latitude", m_latitude);
        obj.put("longitude", m_longitude);
        obj.put("altitude", m_altitude);
        obj.put("timezone", m_timezone);
        return obj;
    }

    /**
     * @return the m_name
     */
    public String getName() {
        return m_name;
    }

    /**
     * @return the m_city
     */
    public String getCity() {
        return m_city;
    }

    /**
     * @return the m_country
     */
    public String getCountry() {
        return m_country;
    }

    /**
     * @return the m_code
     */
    public String getCode() {
        return m_code;
    }

    /**
     * @return the m_latitude
     */
    public Float getLatitude() {
        return m_latitude;
    }

    /**
     * @return the m_longitude
     */
    public Float getLongitude() {
        return m_longitude;
    }

    /**
     * @return the m_altitude
     */
    public Integer getAltitude() {
        return m_altitude;
    }

    /**
     * @return the m_timezone
     */
    public String getTimezone() {
        return m_timezone;
    }
}
