package main.java.com.bag.exceptions;

/**
 * Exception thrown when outdated data is found on a read.
 */
public class OutDatedDataException extends Exception
{
    /**
     * Standard Constructor.
     */
    public OutDatedDataException()
    {
        super();
    }

    /**
     * Constructor with the simple message.
     * @param message incoming message.
     */
    public OutDatedDataException(String message) { super(message); }

    /**
     * Constructor with the message and cause.
     * @param message the message.
     * @param cause the cause.
     */
    public OutDatedDataException(String message, Throwable cause) { super(message, cause); }

    /**
     * Constructor with only the cause.
     * @param cause the cause.
     */
    public OutDatedDataException(Throwable cause) { super(cause); }

    /**
     * Checks if an input is a valid snapshotId for the transaction.
     * @param input the input to check.
     * @param snapshotId the real snapshotId.
     * @throws OutDatedDataException exception to throw if not valid.
     */
    public static void checkSnapshotId(Object input, long snapshotId) throws OutDatedDataException
    {
        long tempSnapshotId = 0;

        if(input instanceof String)
        {
            tempSnapshotId = Long.valueOf((String) input);
        }
        else if(input instanceof Long)
        {
            tempSnapshotId = (long) input;
        }

        if(tempSnapshotId > snapshotId)
        {
            throw new OutDatedDataException("Requested node or relationship has been updated by the database since the start of this transaction");
        }
    }
}
