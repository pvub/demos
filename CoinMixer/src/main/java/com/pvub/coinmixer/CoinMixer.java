package com.pvub.coinmixer;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CoinMixer {
    private final Logger        logger;
    private final JsonObject    appConfig;
    private final Scheduler     scheduler;
    private final String        mixerAddress;
    private final String        coinNetwork;
    // HTTP Connection pooling
    private CloseableHttpClient httpClient                              = null;
    // HTTP Connection Pool manager
    private PoolingHttpClientConnectionManager httpConnectionManager    = null;
    // Subject we listen on for scheduled transfers
    private PublishSubject<TransferEnvelope> transferSubject            = PublishSubject.create();
    private Subscription timedTransferSubscription                      = null;
    // Poll for inbound transfers
    private Subscription inboundTransferDetectSubscription              = null;

    // Current state of Mixer inbound address
    private double mixerBalance                                         = 0.0d;
    private long   lastTransactionTimestamp                             = 0;
    private JsonObject mixerAddressInfo                                 = null;

    // Users we track and mix
    // Lookup by User's source address
    private ConcurrentHashMap<String, User> managedUsers                = new ConcurrentHashMap<>();

    // Date & Time parsing
    static  final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    public CoinMixer(JsonObject appConfig, Scheduler scheduler) {
        this.logger         = LoggerFactory.getLogger("MIXER");
        this.appConfig      = appConfig;
        this.mixerAddress   = appConfig.getString("mixer-address");
        this.coinNetwork    = appConfig.getString("coin-network");
        this.scheduler      = scheduler;

        httpConnectionManager = new PoolingHttpClientConnectionManager();
        httpConnectionManager.setMaxTotal(5);
        httpConnectionManager.setDefaultMaxPerRoute(5);
        httpClient =
                HttpClientBuilder.create()
                        .setConnectionManager(httpConnectionManager)
                        .setDefaultRequestConfig(
                                RequestConfig.custom()
                                        .setConnectTimeout(5 * 1000)
                                        .setSocketTimeout(5 * 1000)
                                        .build())
                        .build();
    }

    public void addUser(User user) {
        managedUsers.put(user.getDepositAddress(), user);
    }

    private void mix(User user, double amount) {
        // Transfer amount from User's address to mixer's own address
        transfer(user.getDepositAddress(), mixerAddress, amount);
        // Schedule mix transactions from mixer address to user's destination addresses
        ArrayList<String> userDestinationAddresses = user.getDestinationAddress();
        // Get the amount and split equally between addresses
        int destinationCount = userDestinationAddresses.size();
        double fractionAmount = amount / destinationCount;
        // Schedule transfers for each address
        for (String transferAddress : userDestinationAddresses) {
            TransferEnvelope envelope = new TransferEnvelope(mixerAddress, transferAddress, fractionAmount);
            logger.info("Scheduling transfer {} coins {}->{}", fractionAmount, mixerAddress, transferAddress);
            transferSubject.onNext(envelope);
        }
    }

    public boolean transfer(String sourceAddress, String destinationAddress, double amount) {
        HttpPost httpPost = null;
        try {
            URIBuilder uriBuilder = new URIBuilder(coinNetwork + "/transactions");
            ArrayList<NameValuePair> postParameters = new ArrayList<NameValuePair>();
            postParameters.add(new BasicNameValuePair("fromAddress", sourceAddress));
            postParameters.add(new BasicNameValuePair("toAddress", destinationAddress));
            postParameters.add(new BasicNameValuePair("amount", String.valueOf(amount)));
            httpPost = new HttpPost(uriBuilder.build());
            httpPost.setEntity(new UrlEncodedFormEntity(postParameters, "UTF-8"));
        } catch (URISyntaxException urie) {
            logger.error("Error building URI", urie);
        } catch (UnsupportedEncodingException ex) {
            logger.error("Error encoding", ex);
        }

        //Execute and get the response.
        HttpResponse response = null;
        HttpEntity entity = null;
        try {
            response = httpClient.execute(httpPost);
            entity = response.getEntity();
        } catch (IOException ex) {
            logger.error("Error executing http request {}->{}",sourceAddress, destinationAddress, ex);
        }

        if (response.getStatusLine().getStatusCode() == 422) {
            // Insufficient funds
            logger.error("Insufficient funds {}->{}", sourceAddress, destinationAddress);
            return false;
        }

        if (entity != null) {
            try {
                String content =  EntityUtils.toString(entity);
                logger.info("Result={}", content);
            } catch (IOException ex) {
                logger.error("Error reading response {}->{}",sourceAddress, destinationAddress, ex);
            } catch (ParseException ex) {
                logger.error("Error parsing {}->{}",sourceAddress, destinationAddress, ex);
            }
        }

        logger.info("Transferred {} coins from {} to {}", amount, sourceAddress, destinationAddress);
        return true;
    }

    public double balance(String userAddress) {
        logger.info("Fetching balance at {}", userAddress);
        JsonObject addressInfo = fetch(userAddress);
        return Double.parseDouble(addressInfo.getString("balance"));
    }


    private JsonObject fetch(String address) {
        HttpGet httpGet = null;
        try {
            URIBuilder uriBuilder = new URIBuilder(coinNetwork + "/addresses/" + address);
            httpGet = new HttpGet(uriBuilder.build());
        } catch (URISyntaxException urie) {
            logger.error("Error building URI", urie);
        }

        //Execute and get the response.
        HttpResponse response = null;
        HttpEntity entity = null;
        try {
            response = httpClient.execute(httpGet);
            entity = response.getEntity();
        } catch (IOException ex) {
            logger.error("Error executing http request", ex);
        }

        JsonObject responseObj = null;
        if (entity != null) {
            try {
                String content =  EntityUtils.toString(entity);
                logger.info("Result={}", content);
                responseObj = new JsonObject(content);
            } catch (IOException ex) {
                logger.error("Error reading response", ex);
            } catch (ParseException ex) {
                logger.error("Error parsing", ex);
            }
        }

        return responseObj;
    }

    public void start() {
        inboundTransferDetectSubscription =
                Observable.interval(5, TimeUnit.SECONDS)
                        .subscribeOn(Schedulers.io())
                        .subscribe(t -> {
                                    detectInboundTransfer();
                                },
                                error -> {
                                    logger.error("Error fatching deposit address", error);
                                },
                                () -> {});
        timedTransferSubscription =
            transferSubject.zipWith(Observable.interval(500, TimeUnit.MILLISECONDS)
                                              .onBackpressureDrop(),
                                    (item, interval) -> item)
                            .observeOn(scheduler)
                            .subscribe(envelope -> {
                                        transfer(envelope.getSourceAddress(),
                                                envelope.getDestinationAddress(),
                                                envelope.getAmount());
                                    },
                                    error -> {
                                        logger.error("Error in transfer subscription", error);
                                    },
                                    () -> {});
    }

    public void stop() {
        if (inboundTransferDetectSubscription != null) {
            inboundTransferDetectSubscription.unsubscribe();
            inboundTransferDetectSubscription = null;
        }
        if (timedTransferSubscription != null) {
            timedTransferSubscription.unsubscribe();
            timedTransferSubscription = null;
        }
    }

    private void detectInboundTransfer() {
        // For each managed User, fetch their deposit address for new transactions
        Observable.from(managedUsers.entrySet())
                .observeOn(scheduler)
                .subscribe(
                        userEntry -> {
                            processUserAddress(userEntry.getKey(), userEntry.getValue());
                        },
                        error -> {
                            logger.error("Error detecting inbound transfers", error);
                        },
                        () -> {});
    }

    private void processUserAddress(String userAddress, User user) {
        logger.info("Fetching transaction for {}@{} LastTX@{}",
                user.getName(), userAddress, user.getLastTransactionTimestamp());
        JsonObject addressInfo = fetch(userAddress);
        // If we are able to pull info from the user address, process transactions
        if (addressInfo != null) {
            processUserInboundTransactions(addressInfo, userAddress, user);
        }
    }

    private void processUserInboundTransactions(JsonObject addressInfo, String userAddress, User user) {
        String incomingBalanceStr = addressInfo.getString("balance");
        if (incomingBalanceStr != null && !incomingBalanceStr.isEmpty()) {
            // Get balance converted from String
            double userBalance = Double.parseDouble(incomingBalanceStr);
            if (userBalance > 0.0d) {
                // Since we don't have a record of where we left before
                // We'll just use the balance
                if (user.getLastTransactionTimestamp() == 0) {
                    // We have a deposit from a managed user
                    user.setLastTransactionTimestamp(new Date().getTime());
                    mix(user, userBalance);
                } else {
                    // Process Transactions
                    // Ignore transactions already processed
                    JsonArray transactionsArray = addressInfo.getJsonArray("transactions");
                    if (transactionsArray != null && !transactionsArray.isEmpty()) {
                        for (int index = 0; index < transactionsArray.size(); ++index) {
                            JsonObject transactionObj = (JsonObject) transactionsArray.getJsonObject(index);
                            processUserInboundTransaction(transactionObj, userAddress, user);
                        }
                    }
                }
            }
        }
        mixerAddressInfo = addressInfo;
    }

    private void processUserInboundTransaction(JsonObject transactionObj, String userAddress, User user) {
        // New Coins created
        if (!transactionObj.containsKey("fromAddress")) {
            // New coins
        }
        // Look for transactions coming in
        if (transactionObj.containsKey("toAddress") && transactionObj.containsKey("fromAddress")) {
            String toAddress    = transactionObj.getString("toAddress");
            String fromAddress  = transactionObj.getString("fromAddress");
            long   ts           = getTransactionTime(transactionObj.getString("timestamp"));
            double amount       = Double.parseDouble(transactionObj.getString("amount"));
            // Check for source addresses we care about
            if (ts > user.getLastTransactionTimestamp()
                && toAddress.equalsIgnoreCase(userAddress))
            {
                // We have a deposit from a managed user
                user.setLastTransactionTimestamp(ts);
                mix(user, amount);
            }
        }
    }

    private long getTransactionTime(String timestampStr) {
        long ts = 0;
        LocalDateTime localDateTime = LocalDateTime.parse(timestampStr.replaceAll("Z$", "+0000"), dateFormatter);
        ts = localDateTime.atZone(ZoneId.of("America/Chicago")).toInstant().toEpochMilli();
        return ts;
    }
}
