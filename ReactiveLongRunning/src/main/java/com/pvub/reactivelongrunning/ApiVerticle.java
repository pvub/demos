package com.pvub.reactivelongrunning;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import java.util.Stack;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * Verticle to handle API requests
 * @author Udai
 */
public class ApiVerticle extends AbstractVerticle {

    private final Logger    m_logger;
    private Router          m_router;
    private Integer         m_max_delay_milliseconds;
    private JsonObject      m_config = null;
    private ExecutorService m_worker_executor = null;
    private Scheduler       m_scheduler;
    private AtomicLong      m_latency = new AtomicLong(0);
    private Integer         m_worker_count = 0;
    private AtomicInteger   m_current_workers = new AtomicInteger(0);
    private Stack<String>   m_deployed_verticles = new Stack<String>();
    private ConcurrentHashMap<String, ClientConnection> m_client_connections = new ConcurrentHashMap<>();

    public ApiVerticle() {
        super();
        m_logger = LoggerFactory.getLogger("APIVERTICLE");
    }
    @Override
    public void start() throws Exception {
        m_logger.info("Starting ApiVerticle");

        String path_to_config = System.getProperty("reactiveapi.config", "conf/config.json");
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
            processConfigChange(previous, conf);
        });
    }

    @Override
    public void stop() throws Exception {
        m_logger.info("Stopping ApiVerticle");
        m_worker_executor.shutdown();
    }

    private void processConfig(JsonObject config) {
        m_config = config;
        m_worker_count           = m_config.getInteger("worker-count", 1);
        m_max_delay_milliseconds = m_config.getInteger("max-delay-milliseconds", 1000);
        Integer worker_pool_size = m_config.getInteger("worker-pool-size", Runtime.getRuntime().availableProcessors() * 2);
        m_logger.info("max_delay_milliseconds={} worker_pool_size={}", m_max_delay_milliseconds, worker_pool_size);
        if (m_worker_executor == null)
        {
            m_worker_executor = Executors.newFixedThreadPool(worker_pool_size);
        }
        if (m_scheduler == null)
        {
            m_scheduler = Schedulers.from(m_worker_executor);
        }
    }

    private void processConfigChange(JsonObject prev, JsonObject current) {
        if (prev.getInteger("worker-count", 1) != current.getInteger("worker-count", 1)) {
            m_worker_count = current.getInteger("worker-count", 1);
            deployWorkers(m_worker_count);
        }
    }

    private void startup(JsonObject config) {
        processConfig(config);

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
        m_router.get("/api/transaction/:customer/:tid").handler(this::handleTransaction);
        m_router.route("/static/*").handler(StaticHandler.create());

        // Create the SockJS handler. Specify options.
        SockJSHandlerOptions options = new SockJSHandlerOptions()
                .setHeartbeatInterval(2000)
                .setRegisterWriteHandler(true); // We need an identifier
        SockJSHandler ebHandler = SockJSHandler.create(vertx, options);

        // Our websocket endpoint: /eventbus/
        m_router.route("/eventbus/*").subRouter(ebHandler.socketHandler(sockJSSocket -> {
            // Extract the identifier
            final String id = sockJSSocket.writeHandlerID();
            // Create an object to map the client socket
            ClientConnection connection = new ClientConnection(id, sockJSSocket, vertx.eventBus());
            // Keep track of open connections
            m_client_connections.put(id, connection);
            // Register for end callback
            sockJSSocket.endHandler((Void) -> {
                connection.stop();
                m_client_connections.remove(id);
            });
            // Start the connection
            connection.start();
        }));

        int port = m_config.getInteger("port", 8080);
        // Create the HTTP server and pass the
        // "accept" method to the request handler.
        vertx
                .createHttpServer()
                .requestHandler(m_router)
                .listen(
                        // Retrieve the port from the
                        // configuration, default to 8080.
                        port,
                        result -> {
                            if (result.succeeded()) {
                                m_logger.info("Listening now on port {}", port);
                                deployWorkers(m_worker_count);
                            } else {
                                m_logger.error("Failed to listen", result.cause());
                            }
                        }
                );
    }

    private void handleTransaction(RoutingContext rc) {
        HttpServerResponse response = rc.response();
        String customer = rc.pathParam("customer");
        String tid = rc.pathParam("tid");
        long startTS = System.nanoTime();

        JsonObject requestObject = new JsonObject();
        requestObject.put("tid", tid);
        requestObject.put("customer", customer);
        requestObject.put("status", TransactionStatus.PENDING.value());
        vertx.eventBus().request("WORKER", requestObject.encode(), result -> {
            if (result.succeeded()) {
                String resp = result.result().body().toString();
                m_logger.info("Sending response for request {} {}", tid, resp);
                m_latency.addAndGet((System.nanoTime() - startTS)/1000000);
                if (response.closed() || response.ended()) {
                    m_logger.info("response closed");
                    return;
                }
                response
                        .setStatusCode(201)
                        .putHeader("content-type",
                                "application/json; charset=utf-8")
                        .end(resp);
            } else {
                m_logger.info("Sending error response for request {}", tid);
                m_latency.addAndGet((System.nanoTime() - startTS)/1000000);
                if (response.closed() || response.ended()) {
                    return;
                }
                response
                        .setStatusCode(404)
                        .putHeader("content-type",
                                "application/json; charset=utf-8")
                        .end();
            }
        });

    }
    public void generateHealth(RoutingContext ctx) {
        ctx.response()
                .setChunked(true)
                .putHeader("Content-Type", "application/json;charset=UTF-8")
                .putHeader("Access-Control-Allow-Methods", "GET")
                .putHeader("Access-Control-Allow-Origin", "*")
                .putHeader("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type")
                .setStatusCode(HttpResponseStatus.OK.code())
                .write((new JsonObject().put("status", "OK")).encode());
    }

    private void deployWorkers(int count) {
        if (count > m_current_workers.get()) {
            while (count > m_current_workers.get()) {
                addWorker();
            }
        } else if (count < m_current_workers.get()) {
            while (count < m_current_workers.get()) {
                removeWorker();
            }
        }
    }

    private void addWorker() {
        m_current_workers.incrementAndGet();
        JsonObject config = new JsonObject().put("instance", m_current_workers.get());
        DeploymentOptions workerOpts = new DeploymentOptions()
                .setConfig(config)
                .setWorker(true)
                .setInstances(1)
                .setWorkerPoolSize(1);
        vertx.deployVerticle(ApiWorkerVerticle.class.getName(), workerOpts, res -> {
            if(res.failed()){
                m_logger.error("Failed to deploy worker verticle {}", ApiWorkerVerticle.class.getName(), res.cause());
            }
            else {
                String depId = res.result();
                m_deployed_verticles.add(depId);
                m_logger.info("Deployed verticle {} DeploymentID {}", ApiWorkerVerticle.class.getName(), depId);
            }
        });
    }

    private void removeWorker() {
        m_current_workers.decrementAndGet();
        String id = m_deployed_verticles.pop();
        m_logger.info("Undeploying ID {} #WorkerVerticles {}", id, m_current_workers.get());
        vertx.undeploy(id);
    }
}
