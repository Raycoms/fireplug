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
    public static final String NEO4J                      = "neo4j";
    public static final String ORIENTDB                   = "orientDB";
    public static final String TITAN                      = "titan";
    public static final String SPARKSEE                   = "sparksee";
    public static final String TAG_SNAPSHOT_ID            = "snapShotId";
    public static final String COMMIT                     = "commit";
    public static final String ABORT                      = "abort";
    public static final String TAG_HASH                   = "hash";
    public static final String GET_PRIMARY                = "getPrimary";
    public static final String SIGNATURE_MESSAGE         = "signatures";
    public static final String REGISTER_GLOBALLY_MESSAGE = "registering";
    public static final String REGISTER_GLOBALLY_CHECK   = "registeringReply";
    public static final String REGISTER_GLOBALLY_REPLY   = "registeringReply";
    public static final String UPDATE_SLAVE              = "updateSlave";
    public static final String CONTINUE                  = "continue";
    public static final String TAG_VERSION = "version";
    public static final String TAG_PRE = "preversion";

    /**
     * Used to convert nano time to seconds.
     */
    public static final double NANO_TIME_DIVIDER = 1000000000.0;

    public static final String[] RELATIONSHIP_TYPES_LIST = {"hasInterest",
            "hasModerator",
            "hasMember",
            "studyAt",
            "worksAt",
            "isLocatedIn",
            "isPartOf",
            "likes",
            "hasCreator",
            "containerOf",
            "hasTag",
            "hasType",
            "isSubclassOf",
            "replyOf",
            "creationDate",
            "name",
            "gender",
            "birthday",
            "email",
            "speaks",
            "browserUsed",
            "locationIP",
            "content",
            "language",
            "imageFile", "length"};

    /**
     * Used to hide the implicit default constructor.
     */
    private Constants()
    {
        /*
         * Intentionally left empty.
         */
    }
}
