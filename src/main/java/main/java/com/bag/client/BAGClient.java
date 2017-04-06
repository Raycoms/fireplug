package main.java.com.bag.client;

import java.util.concurrent.BlockingQueue;

/**
 * Interface with operations that a BAG Client has to implement
 */
public interface BAGClient {

    /**
     * Get the blocking queue.
     * @return the queue.
     */
    BlockingQueue<Object> getReadQueue();

    /**
     * write requests. (Only reach database on commit)
     */
    void write(final Object identifier, final Object value);

    /**
     * ReadRequests.(Directly read database) send the request to the db.
     * @param identifiers list of objects which should be read, may be NodeStorage or RelationshipStorage
     */
    void read(final Object...identifiers);

    /**
     * Commit reaches the server, if secure commit send to all, else only send to one
     */
    public void commit();
}
