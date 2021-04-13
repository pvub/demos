package com.pvub.coinmixer;

public class TransferEnvelope {
    private String sourceAddress;
    private String destinationAddress;
    private double amount;

    public TransferEnvelope(String sourceAddress, String destinationAddress, double amount) {
        this.sourceAddress = sourceAddress;
        this.destinationAddress = destinationAddress;
        this.amount = amount;
    }

    public String getSourceAddress() {
        return sourceAddress;
    }

    public String getDestinationAddress() {
        return destinationAddress;
    }

    public double getAmount() {
        return amount;
    }
}
