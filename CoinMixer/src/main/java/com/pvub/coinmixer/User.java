package com.pvub.coinmixer;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;

public class User {
    private final String name;
    private final String depositAddress;
    private final ArrayList<String> destinationAddress;
    private       double balance;
    private       long   lastTransactionTimestamp = 0;

    public User(String name, String depAddress, ArrayList<String> destinations) {
        this.name = name;
        this.depositAddress = depAddress;
        this.destinationAddress = destinations;
        this.balance = 0.0d;
    }

    public User(JsonObject userObj) {
        this.name = userObj.getString("name");
        this.balance = Double.parseDouble(userObj.getString("initialBalance"));
        this.depositAddress = userObj.getString("sourceAddress");
        this.destinationAddress = new ArrayList<String>();
        JsonArray addressArray = userObj.getJsonArray("destinationAddresses");
        for (int index = 0; index < addressArray.size(); ++index) {
            String address = addressArray.getString(index);
            destinationAddress.add(address);
        }
    }

    public String getName() {
        return name;
    }

    public String getDepositAddress() {
        return depositAddress;
    }

    public ArrayList<String> getDestinationAddress() {
        return destinationAddress;
    }

    public double getBalance() {
        return balance;
    }

    public long getLastTransactionTimestamp() {
        return lastTransactionTimestamp;
    }

    public void setLastTransactionTimestamp(long lastTransactionTimestamp) {
        this.lastTransactionTimestamp = lastTransactionTimestamp;
    }

    public void setBalance(double balance) {
        this.balance = balance;
    }
}
