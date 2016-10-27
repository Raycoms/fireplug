package main.java.com.bag.server.database.Interfaces;

/**
 * Abstract class with required methods for all graph databases.
 */
public abstract interface IDatabaseAccess
{
    /**
     * Method used to start the graph database service.
     * @param id change foldername depending on the id.
     */
    public void start(int id);

    /**
     * Method used to terminate the graph database service.
     */
    public void terminate();

}
