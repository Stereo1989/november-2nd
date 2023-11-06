package com.glodon.base.async;

public interface AsyncTask extends Runnable {

    int MIN_PRIORITY = 1;
    int NORM_PRIORITY = 5;
    int MAX_PRIORITY = 10;

    default int getPriority() {
        return NORM_PRIORITY;
    }

    default boolean isPeriodic() {
        return false;
    }
}
