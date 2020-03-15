package com.pvub.disruptordemo;

import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.web.handler.sockjs.BridgeOptions;
import io.vertx.ext.web.handler.sockjs.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler;
import rx.schedulers.Schedulers;
import io.vertx.core.eventbus.MessageConsumer;

/**
 * Verticle to handle API requests
 * @author Udai
 */
public class MainVerticle extends AbstractVerticle {
    private final Logger    m_logger;
    private Router          m_router;
    private JsonObject      m_config = null;
    private ExecutorService m_worker_executor = null;
    private Scheduler       m_scheduler;
    private HashMap<String, String>   m_deployed_verticles = new LinkedHashMap<String, String>();
    private MessageConsumer<String> m_console_consumer;
    
    public MainVerticle() {
        super();
        m_logger = LoggerFactory.getLogger("MAIN");
    }
    @Override
    public void start() throws Exception {
        m_logger.info("Starting MainVerticle");

        ConfigRetriever retriever = ConfigRetriever.create(vertx);
        retriever.getConfig(
            config -> {
                m_logger.info("config retrieved");
                if (config.failed()) {
                    m_logger.info("No config");
                } else {
                    m_logger.info("Got config");
                    m_config = config.result();
                    m_worker_executor = Executors.newSingleThreadExecutor();
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
                    
                    BridgeOptions opts = new BridgeOptions()
                      .addInboundPermitted(new PermittedOptions().setAddress("console.to.app"))
                      .addOutboundPermitted(new PermittedOptions().setAddress("app.to.console"));

                    SockJSHandler sockJSHandler = SockJSHandler.create(vertx);
                    sockJSHandler.bridge(opts, event -> {
                      // This signals that it's ok to process the event
                      event.complete(true);
                    });
                    m_router.route("/eventbus/*").handler(sockJSHandler);
                    
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
                                    startup();
                                } else {
                                    m_logger.error("Failed to listen", result.cause());
                                }
                            }
                        );
                    
                    // Listen to incoming requests from console and respond with data
                    m_console_consumer = vertx.eventBus().consumer("console.to.app");
                    m_console_consumer.handler(message -> {
                        processConsoleMessage(message);
                    });
                }
            }
        );
    }
    
    @Override
    public void stop() throws Exception {
        m_logger.info("Stopping MainVerticle");
    }
    
    private void startup() {
    }
    
    private void deployVerticle(String classname, String name) {
        vertx.deployVerticle(classname, (result) -> {
            if(result.failed()){
                m_logger.error("Failed to deploy worker verticle {}", classname, result.cause());
            }
            else {
                String depId = result.result();
                m_deployed_verticles.put(name, depId);
                m_logger.info("Deployed verticle {} DeploymentID {}", classname, depId);
            }
        });
    }
    private void stopVerticle(String name) {
        String deploymentID = m_deployed_verticles.get(name);
        if (deploymentID == null)
            return;
        if (vertx.deploymentIDs().contains(deploymentID)) {
            m_logger.info("Undeploying: " + deploymentID);
            vertx.undeploy(deploymentID, res2 -> {
                if (res2.succeeded()) {
                    m_logger.info("{} {} verticle undeployed", deploymentID, name);
                } else {
                    res2.cause().printStackTrace();
                }
            });
            m_deployed_verticles.remove(name);
        }
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

    private void processConsoleMessage(Message<String> message) {
        String json_request = message.body();
        m_logger.info("Console message=" + json_request);
        JsonObject json_request_obj = new JsonObject(json_request);
        String  design = json_request_obj.getString("design");
        if (m_deployed_verticles.get(design) == null) {
            // Star
            deployVerticle(getVerticle(design), design);
        } else {
            // Stop
            stopVerticle(design);
        }
    }
    
    private String getVerticle(String name) {
        if (name.compareToIgnoreCase("s2srx") == 0) {
            return com.pvub.disruptordemo.SingleRxProducerVerticle.class.getName();
        }
        else if (name.compareToIgnoreCase("m2srx") == 0) {
            return com.pvub.disruptordemo.MultiRxProducerVerticle.class.getName();
        }
        else if (name.compareToIgnoreCase("s2sdrp") == 0) {
            return com.pvub.disruptordemo.SingleProducerVerticle.class.getName();
        }
        else if (name.compareToIgnoreCase("m2sdrp") == 0) {
            return com.pvub.disruptordemo.MultiProducerVerticle.class.getName();
        }
        else return null;
    }
}