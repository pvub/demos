package com.pvub.graphdemo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.internal.InternalRelationship;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.InternalPair;
import org.neo4j.driver.internal.value.NodeValue;
import org.neo4j.driver.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Graph DB powered by embedded Neo4j
 * @author Udai
 */
public class Graph {
    
    private static String ROUTE_QUERY = "MATCH p=(src:Airport {code:$source})-[r:Connects*"
                                      + "ccc"
                                      + "]->(dst:Airport {code:$destination}) "
                                      + "WITH reduce(output = [], n IN relationships(p) | output + n ) as nodeCollection "
                                      + "RETURN nodeCollection";
    private static String AIRPORT_QUERY = "MATCH (airport:Airport {code:$code}) return airport";
    private static String ALL_AIRPORT_QUERY = "MATCH (airport:Airport) return airport";
    
    
    private final Logger        m_logger;
    private Driver              m_driver = null;
    private Session             m_session = null;
    private Airport             m_airport = null;
    
    public Graph() {
        m_logger = LoggerFactory.getLogger("GRAPH");
        startup();
    }
    
    private void startup() {
        m_driver = GraphDatabase.driver("bolt://localhost:7687"
                                        , AuthTokens.basic("neo4j", "password"));
        m_logger.info("Got driver");
        m_session = m_driver.session();
        m_logger.info("Got session");
        m_logger.info("neo4j session created {}", m_session.isOpen());
    }
    
    public void stop() {
        m_session.close();
        m_driver.close();
    }
    
    public void createNode(Node pojo) {
        String createStr = pojo.getCreateCypher();
        m_session.run(createStr, pojo.getCreateParams());
    }
    
    public List<AirPath> getRoutes(String src, String dst, int connections) {
        String query = ROUTE_QUERY.replace("ccc", "1.." + connections);
        m_logger.info("query={}", query);
        List<AirPath> airpaths = new ArrayList<AirPath>();
        HashMap<String, Object> params = new HashMap<String, Object>();
        params.put("source", src);
        params.put("destination", dst);
        try
        {
            Result r = m_session.run(query, params);
            r.stream().forEach(record -> {
                record.fields().forEach(p -> {
                    //m_logger.info("Record {} {}", p.key(), p.value());
                    AirPath airpath = new AirPath();
                    p.value().asList().forEach(n -> {
                        InternalRelationship rel = (InternalRelationship)n;
                        String source = rel.get("source").asString();
                        String destination = rel.get("destination").asString();
                        String airline = rel.get("airline").asString();
                        m_logger.info("Route {}-{}-{}", source, destination, airline);
                        Route rte = new Route(source, destination, airline);
                        airpath.add(rte);
                    });
                    airpaths.add(airpath);
                });
            });
        } catch (Exception e) {
            m_logger.error("Route query error {}", query, e);
        }
        return airpaths;
    }
    
    public Airport getAirport(String airportcode) {
        m_airport= null;
        HashMap<String, Object> params = new HashMap<String, Object>();
        params.put("code", airportcode);
        try
        {
            Result r = m_session.run(AIRPORT_QUERY, params);
            r.stream().forEach(record -> {
                List<Pair<String,Value>> values = record.fields();
                for (Pair<String,Value> pair: values) {
                    if (pair.key().compareToIgnoreCase("airport") == 0) {
                        Value node = pair.value();
                        m_airport = new Airport(node);
                    }
                }
            });
        } catch (Exception e) {
            m_logger.error("Airport query error {} code={}", AIRPORT_QUERY, airportcode, e);
        }
        return m_airport;
    }
    
    public List<Airport> getAllAirports() {
        List<Airport> airports = new ArrayList<Airport>();
        try
        {
            Result r = m_session.run(ALL_AIRPORT_QUERY);
            r.stream().forEach(record -> {
                List<Pair<String,Value>> values = record.fields();
                for (Pair<String,Value> pair: values) {
                    if (pair.key().compareToIgnoreCase("airport") == 0) {
                        Value node = pair.value();
                        Airport airport = new Airport(node);
                        airports.add(airport);
                    }
                }
            });
        } catch (Exception e) {
            m_logger.error("All Airport query error {}", ALL_AIRPORT_QUERY, e);
        }
        return airports;
    }
}
