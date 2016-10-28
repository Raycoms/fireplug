package main.java.com.bag.util;

/**
 * Class holding the basic constants for the execution.
 */
public class Constants
{
    public static final String COMMIT_MESSAGE             = "commit";
    public static final String READ_MESSAGE               = "node/read";
    public static final String RELATIONSHIP_READ_MESSAGE  = "relationship/read";
    public static final String COMMIT_RESPONSE            = "commit/response";
    public static final String NODE_READ_RESPONSE         = "node/read/response";
    public static final String RELATIONSHIP_READ_RESPONSE = "relationship/read/response";
    public static final String NEO4J                      = "neo4";
    public static final String ORIENTDB                   = "orientDB";
    public static final String TITAN                      = "titan";
    public static final String ARANGODB                   = "arangoDB";


    /**
     * Used to hide the implicit default constructor.
     */
    private Constants()
    {
        /**
         * Intentionally left empty.
         */
    }
}
