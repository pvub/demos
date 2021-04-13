package com.pvub.coinmixer;

import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.ext.web.RoutingContext;
import io.vertx.rxjava.config.ConfigRetriever;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.ext.web.Router;
import io.vertx.rxjava.ext.web.handler.CorsHandler;
import io.vertx.rxjava.ext.web.handler.StaticHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Scheduler;
import rx.schedulers.Schedulers;

/**
 * Verticle to handle API requests
 * @author Udai
 */
public class MixerVerticle extends AbstractVerticle {
    static final int WORKER_POOL_SIZE = 20;

    private final Logger    logger;
    private Router          router;
    private JsonObject      appConfig = null;
    private ExecutorService worker_executor = null;
    private Scheduler       scheduler;

    private CoinMixer       mixer = null;

    public MixerVerticle() {
        super();
        logger = LoggerFactory.getLogger("MIXERVERTICLE");
    }
    @Override
    public void start() throws Exception {
        logger.info("Starting MixerVerticle");

        ConfigRetriever retriever = null;
        String path_to_config = System.getProperty("mixer.config", "");
        if (!path_to_config.isEmpty()) {
            ConfigStoreOptions fileStore = new ConfigStoreOptions()
                    .setType("file")
                    .setFormat("json")
                    .setConfig(new JsonObject().put("path", path_to_config));

            retriever = ConfigRetriever.create(vertx,
                    new ConfigRetrieverOptions().addStore(fileStore));
        }
        if (retriever == null) {
            logger.error("Unable to create config retriever");
            return;
        }
        retriever.getConfig(
                config -> {
                    logger.info("config retrieved");
                    if (config.failed()) {
                        logger.info("No config");
                    } else {
                        logger.info("Got config");
                        appConfig = config.result();
                        Integer worker_pool_size = appConfig.getInteger("worker-pool-size", Runtime.getRuntime().availableProcessors() * 2);
                        worker_executor = Executors.newFixedThreadPool(worker_pool_size);
                        scheduler = Schedulers.from(worker_executor);
                        // Create a router object.
                        router = Router.router(vertx);

                        // Handle CORS requests.
                        router.route().handler(CorsHandler.create("*")
                                .allowedMethod(HttpMethod.GET)
                                .allowedMethod(HttpMethod.OPTIONS)
                                .allowedHeader("Accept")
                                .allowedHeader("Authorization")
                                .allowedHeader("Content-Type"));

                        router.get("/api/deposit").handler(this::deposit);
                        router.route("/static/*").handler(StaticHandler.create());

                        int port = appConfig.getInteger("port", 8080);
                        // Create the HTTP server and pass the
                        // "accept" method to the request handler.
                        vertx
                                .createHttpServer()
                                .requestHandler(router::accept)
                                .listen(
                                        // Retrieve the port from the
                                        // configuration, default to 8080.
                                        port,
                                        result -> {
                                            if (result.succeeded()) {
                                                logger.info("Listening now on port {}", port);
                                                initializeMixer();
                                            } else {
                                                logger.error("Failed to listen", result.cause());
                                            }
                                        }
                                );
                    }
                }
        );
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping MixerVerticle");
        if (mixer != null) {
            mixer.stop();
        }
    }

    private void initializeMixer() {
        mixer = new CoinMixer(appConfig, scheduler);
        // add users to mixer
        JsonArray userArray = appConfig.getJsonArray("users");
        for (int index = 0; index < userArray.size(); ++index) {
            JsonObject userObj = userArray.getJsonObject(index);
            User user = new User(userObj);
            mixer.addUser(user);
        }
        // start the mixer
        mixer.start();
    }

    // deposit coins into an address. fromAddress is optional
    private void deposit(RoutingContext rc) {
        final String fromAddress    = rc.request().getParam("fromAddress");
        final String toAddress      = rc.request().getParam("toAddress");
        final String amount         = rc.request().getParam("amount");
        if (toAddress == null || amount == null) {
            rc.response()
                    .setStatusCode(HttpStatus.SC_BAD_REQUEST)
                    .end("Destination Address and amount are required");
            return;
        }

        double transferAmount = Double.parseDouble(amount);
        if (mixer.transfer(fromAddress, toAddress, transferAmount)) {
            rc.response().setStatusCode(HttpStatus.SC_METHOD_FAILURE).end("Transfer failed");
        } else {
            rc.response().setStatusCode(HttpStatus.SC_OK).end("Transfer succeeded");
        }
    }
}
