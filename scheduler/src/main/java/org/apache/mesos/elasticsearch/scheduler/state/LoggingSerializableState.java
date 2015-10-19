package org.apache.mesos.elasticsearch.scheduler.state;

import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * stupid comment. screw you checkstyle
 */
public class LoggingSerializableState implements SerializableState {
    private static final Logger LOGGER = Logger.getLogger(LoggingSerializableState.class.toString());

    private final SerializableState wrapped;

    private int numGets = 0;
    private int numSets = 0;
    private int numDeletes = 0;

    public int getNumGets() {
        return numGets;
    }

    public int getNumSets() {
        return numSets;
    }

    public int getNumDeletes() {
        return numDeletes;
    }

    public LoggingSerializableState(SerializableState wrapped) {
        this.wrapped = wrapped;
    }

    public <T> T get(String key) throws IOException {
        this.numGets++;
        this.log();
        return wrapped.get(key);
    }

    public <T> void set(String key, T object) throws IOException {
        this.numSets++;
        this.log();
        wrapped.set(key, object);
    }

    public void delete(String key) throws IOException {
        this.numDeletes++;
        this.log();
        wrapped.delete(key);
    }

    private void log() {
        LOGGER.info("# gets: " + this.numGets + "; # sets: " + this.numSets + "; # deletes: " + this.numDeletes);
    }
}
