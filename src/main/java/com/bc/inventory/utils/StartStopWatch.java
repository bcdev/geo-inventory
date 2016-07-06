package com.bc.inventory.utils;

public class StartStopWatch {
    private long startTime;
    private long sum = 0;

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public void stop() {
        long delta = System.currentTimeMillis() - startTime;
        sum = sum + delta;
    }

    public long getSum() {
        return sum;
    }
}
