package com.pvub.graphdemo;

import com.opencsv.CSVReader;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.config.ConfigRetriever;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.http.HttpServerResponse;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.ext.web.handler.CorsHandler;
import io.vertx.rxjava.ext.web.handler.StaticHandler;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.rxjava.core.eventbus.Message;
import io.vertx.rxjava.ext.web.handler.sockjs.SockJSHandler;

/**
 * Verticle to handle API requests
 * @author Udai
 */
public class APIVerticle extends AbstractVerticle {
    static final int WORKER_POOL_SIZE = 20;

    private final Logger    m_logger;
    private Router          m_router;
    private Integer         m_max_delay_milliseconds;
    private JsonObject      m_config = null;
    private ExecutorService m_worker_executor = null;
    private Scheduler       m_scheduler;
    private AtomicLong      m_latency = new AtomicLong(0);
    private Graph           m_graph = null;
    
    public APIVerticle() {
        super();
        m_logger = LoggerFactory.getLogger("APIVERTICLE");
    }
    
    @Override
    public void start() throws Exception {
        String path_to_config = System.getProperty("graph.config", "conf/api.json");
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
            .setType("file")
            .setFormat("json")
            .setConfig(new JsonObject().put("path", path_to_config));
        
        ConfigRetriever retriever = ConfigRetriever.create(vertx, 
                new ConfigRetrieverOptions().addStore(fileStore));
        retriever.getConfig(
            config -> {
                m_logger.info("config retrieved");
                if (config.failed()) {
                    m_logger.info("No config");
                } else {
                    m_logger.info("Got config");
                    startup(config.result());
                }
            }
        );
        
        retriever.listen(change -> {
            m_logger.info("config changed");
            // Previous configuration
            JsonObject previous = change.getPreviousConfiguration();
            // New configuration
            JsonObject conf = change.getNewConfiguration();
        });
    }
    
    public void startup(JsonObject config) {
        m_config = config;
        m_logger.info("Starting APIVerticle");

        m_max_delay_milliseconds = m_config.getInteger("max-delay-milliseconds", 1000);
        Integer worker_pool_size = m_config.getInteger("worker-pool-size", Runtime.getRuntime().availableProcessors() * 2);
        m_worker_executor = Executors.newFixedThreadPool(worker_pool_size);
        m_scheduler = Schedulers.from(m_worker_executor);

        // Create a router object.
        m_router = Router.router(vertx);

        // Handle CORS requests.
        m_router.route().handler(CorsHandler.create("*")
            .allowedMethod(HttpMethod.GET)
            .allowedMethod(HttpMethod.OPTIONS)
            .allowedHeader("Accept")
            .allowedHeader("Authorization")
            .allowedHeader("Content-Type"));

        m_router.get("/health").handler(this::generateHealth);
        m_router.route("/static/*").handler(StaticHandler.create());
        m_router.get("/routes").handler(this::listRoutes);
        m_router.get("/airport").handler(this::listAirport);
        m_router.get("/airports").handler(this::listAllAirports);

        // set in and outbound permitted addresses
        // Allow events for the designated addresses in/out of the event bus bridge
        BridgeOptions opts = new BridgeOptions()
          .addInboundPermitted(new PermittedOptions().setAddress("console.to.api"))
          .addOutboundPermitted(new PermittedOptions().setAddress("api.to.console"));

        // Create the event bus bridge and add it to the router.
        SockJSHandler ebHandler = SockJSHandler.create(vertx).bridge(opts);
        m_router.route("/eventbus/*").handler(ebHandler);
        
        int port = m_config.getInteger("port", 8080);
        // Create the HTTP server and pass the 
        // "accept" method to the request handler.
        vertx
            .createHttpServer()
            .requestHandler(m_router::accept)
            .listen(
                // Retrieve the port from the 
                // configuration, default to 8080.
                port,
                result -> {
                    if (result.succeeded()) {
                        m_logger.info("Listening now on port {}", port);
                    } else {
                        m_logger.error("Failed to listen", result.cause());
                    }
                }
            );
        
        m_graph = new Graph();
        
        //load();
        
        // Register to listen for messages coming IN to the server
        vertx.eventBus().consumer("console.to.api").handler(message -> {
            handleConsoleMessage(message);
        });
    }
    
    @Override
    public void stop() throws Exception {
        m_logger.info("Stopping StarterVerticle");
        m_graph.stop();
    }
    
    public void generateHealth(RoutingContext ctx) {
        ctx.response()
            .setChunked(true)
            .putHeader("Content-Type", "application/json;charset=UTF-8")
            .putHeader("Access-Control-Allow-Methods", "GET")
            .putHeader("Access-Control-Allow-Origin", "*")
            .putHeader("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type")
            .write((new JsonObject().put("status", "OK")).encode())
            .end();
    }
    
    private void load() {
        String path = m_config.getString("airports");
        if (path != null) {
            m_logger.info("Loading airports from {}", path);
            loadAirports(path);
        }
        path = m_config.getString("routes");
        if (path != null) {
            m_logger.info("Loading routes from {}", path);
            loadRoutes(path);
        }
    }
    
    private void loadAirports(String path) 
    {
        try 
        {
            CSVReader reader = new CSVReader(new FileReader(path));
            Observable
                .fromIterable(reader)
                .map(
                  csvRow -> {
                      if (csvRow.length < 12) 
                      {
                          return null;
                      }
                      return new Airport(csvRow[1], // name
                                         csvRow[2], // city
                                         csvRow[3], // country
                                         csvRow[4], // IATA code
                                         getFloat(csvRow[6]), // latitude
                                         getFloat(csvRow[7]), // longitude
                                         getInteger(csvRow[8]), // altitude
                                         csvRow[11]); // timezone
                  }
                )
                .subscribe(airport -> {
                            m_graph.createNode(airport);
                            m_logger.info("Airport {} {} {}", airport.getCode(),airport.getCity(),airport.getCountry());
                           }
                          , error -> {
                               m_logger.error("Load Airport Exception: {}", error);
                            }
                          , () -> {});

        } catch (FileNotFoundException ex) {
            m_logger.error("Airports file not found", ex);
        }
    }

    private void loadRoutes(String path) 
    {
        try 
        {
            CSVReader reader = new CSVReader(new FileReader(path));
            Observable
                .fromIterable(reader)
                .map(
                  csvRow -> {
                      if (csvRow.length < 4) 
                      {
                          return null;
                      }
                      return new Route(csvRow[2], // sourceairport
                                         csvRow[4], // destinationairport
                                         csvRow[0]); // airline
                  }
                )
                .subscribe(route -> {
                            m_graph.createNode(route);
                            m_logger.info("Route {}->{} on {}", route.getSource(),route.getDestination(),route.getAirline());
                           }
                          , error -> {
                               m_logger.error("Load Routes Exception: {}", error);
                            }
                          , () -> {});

        } catch (FileNotFoundException ex) {
            m_logger.error("Routes file not found", ex);
        }
    }
    
    public void listRoutes(RoutingContext ctx) {
        HttpServerResponse response = ctx.response();
        String src = ctx.request().getParam("source");
        String dst = ctx.request().getParam("destination");
        m_logger.info("Request for {}->{}", src, dst);
        List<AirPath> airpaths = m_graph.getRoutes(src.toUpperCase(), dst.toUpperCase(), 2);
        JsonArray arr = new JsonArray();
        airpaths.forEach(r -> { arr.add(r.toJson()); });
        JsonObject msg = new JsonObject();
        msg.put("action", "routes");
        msg.put("routes", arr);
        response
                .setStatusCode(200)
                .putHeader("content-type", 
                  "application/json; charset=utf-8")
                .end(msg.encodePrettily());
    }
    private void handleRoutes(JsonObject obj) {
        String origin = obj.getString("origin");
        String destination = obj.getString("destination");
        Integer stops = obj.getInteger("stops");
        m_logger.info("Request for {}->{} Stops {}", origin, destination, stops);
        List<AirPath> airpaths = m_graph.getRoutes(origin.toUpperCase(), destination.toUpperCase(), stops);
        JsonArray arr = new JsonArray();
        airpaths.forEach(r -> { arr.add(r.toJson()); });
        JsonObject msg = new JsonObject();
        msg.put("action", "routes");
        msg.put("routes", arr);
        vertx.eventBus().publish("api.to.console", msg.encode());
    }
    
    public void listAllAirports(RoutingContext ctx) {
        HttpServerResponse response = ctx.response();
        List<Airport> airports = m_graph.getAllAirports();
        JsonArray arr = new JsonArray();
        for (Airport a : airports) { arr.add(a.getJson()); }
        JsonObject msg = new JsonObject();
        msg.put("action", "airports");
        msg.put("airports", arr);
        response
                .setStatusCode(200)
                .putHeader("content-type", 
                  "application/json; charset=utf-8")
                .end(msg.encodePrettily());
    }
    public void handleAllAirports() {
        List<Airport> airports = m_graph.getAllAirports();
        JsonArray arr = new JsonArray();
        for (Airport a : airports) { arr.add(a.getJson()); }
        JsonObject msg = new JsonObject();
        msg.put("action", "airports");
        msg.put("airports", arr);
        vertx.eventBus().publish("api.to.console", msg.encode());
    }
    
    public void listAirport(RoutingContext ctx) {
        HttpServerResponse response = ctx.response();
        String airportCode = ctx.request().getParam("code");
        Airport airport = m_graph.getAirport(airportCode);
        JsonObject msg = new JsonObject();
        msg.put("action", "airport");
        msg.put("airport", airport.getJson());
        response
                .setStatusCode(200)
                .putHeader("content-type", 
                  "application/json; charset=utf-8")
                .end(msg.encodePrettily());
    }
    private void handleAirport(JsonObject obj) {
        String airportCode = obj.getString("code");
        Airport airport = m_graph.getAirport(airportCode);
        JsonObject msg = new JsonObject();
        msg.put("action", "airport");
        msg.put("airport", airport.getJson());
        vertx.eventBus().publish("api.to.console", msg.encode());
    }
    
    private void handleConsoleMessage(Message<Object> m) {
        String message = (String) m.body();
        JsonObject msgObj = new JsonObject(message);
        String action = msgObj.getString("action");
        if (action != null) {
            if (action.compareToIgnoreCase("routes") == 0) {
                handleRoutes(msgObj);
            }
            if (action.compareToIgnoreCase("airport") == 0) {
                handleAirport(msgObj);
            }
            if (action.compareToIgnoreCase("airports") == 0) {
                handleAllAirports();
            }
        }
    }
    
    private static Float getFloat(String str) {
        Float f = 0.0f;
        try {
            f = Float.parseFloat(str);
        } catch (NumberFormatException e) {
            f = Float.NaN;
        }
        return f;
    }
    private static Integer getInteger(String str) {
        Integer i = 0;
        try {
            i = Integer.parseInt(str);
        } catch (NumberFormatException e) {
        }
        return i;
    }

}