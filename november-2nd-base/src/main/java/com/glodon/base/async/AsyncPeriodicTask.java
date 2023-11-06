package com.glodon.base.async;

public interface AsyncPeriodicTask extends AsyncTask {

    @Override
    default boolean isPeriodic() {
        return true;
    }

    @Override
    default int getPriority() {
        return MIN_PRIORITY;
    }
}
