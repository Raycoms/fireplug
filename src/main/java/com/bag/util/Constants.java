package com.bag.util;

/**
 * Class holding the basic constants for the execution.
 */
public class Constants
{
    public static final String COMMIT_MESSAGE               = "commit";
    public static final String READ_MESSAGE                 = "node/read";
    public static final String RELATIONSHIP_READ_MESSAGE    = "relationship/read";
    public static final String COMMIT_RESPONSE              = "commit/response";
    public static final String NEO4J                        = "neo4j";
    public static final String ORIENTDB                     = "orientdb";
    public static final String TITAN                        = "titan";
    public static final String SPARKSEE                     = "sparksee";
    public static final String TAG_SNAPSHOT_ID              = "snapShotId";
    public static final String COMMIT                       = "commit";
    public static final String ABORT                        = "abort";
    public static final String TAG_HASH                     = "hash";
    public static final String GET_PRIMARY                  = "getPrimary";
    public static final String SIGNATURE_MESSAGE            = "signatures";
    public static final String REGISTER_GLOBALLY_MESSAGE    = "registering";
    public static final String REGISTER_GLOBALLY_CHECK      = "registeringReply";
    public static final String REGISTER_GLOBALLY_REPLY      = "registeringReply";
    public static final String UPDATE_SLAVE                 = "updateSlave";
    public static final String CONTINUE                     = "continue";
    public static final String TAG_VERSION                  = "version";
    public static final String TAG_PRE                      = "preversion";
    public static final String LOCAL_CLUSTER                = "local";
    public static final String GLOBAL_CLUSTER               = "global";
    public static final String PRIMARY_ELECTION_MESSAGE     = "primaryElection";
    public static final String PERFORMANCE_UPDATE_MESSAGE   = "performanceUpdate";
    public static final String BFT_PRIMARY_ELECTION_MESSAGE = "bftPrimaryElection";
    public static final String AM_I_OUTDATED_MESSAGE        = "amIOutdated?";
    public static final String WRITE_REQUEST                = "writeRequest";
    public static final String ALG_CHANGE                   = "algChange";
    public static final String SLAVE_REQUEST_ALG_CHANGE     = "slaveReqAlgChange";
    public static final String PRIMARY_CONSULT_ALG_CHANGE   = "primaryConsultAlgChange";
    public static final String CONSULT_SLAVE_ALG_CHANGE     = "consultSlaveAlgChange";
    public static final String SLAVE_VOTE                   = "consultSlaveAlgChange";

    /**
     * Border CPU usage to reconfigure to other algorithm.
     */
    public static final int BORDER_CPU_USAGE = 50;

    /**
     * Used to convert nano time to seconds.
     */
    public static final double NANO_TIME_DIVIDER = 1000000000.0;

    public static final String[] RELATIONSHIP_TYPES_LIST =
            {
                    "hasInterest",
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
                    "imageFile", "length"
            };

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
