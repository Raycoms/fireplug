package main.java.com.bag.util.storage;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * Signature storage class. Used to store an amount of signatures.
 */
public class SignatureStorage implements Serializable
{
    /**
     * The message which has been signed with the signatures.
     */
    private byte[] message;

    /**
     * The stored signatures.
     */
    private final Map<Integer, byte[]> signatures = new TreeMap<>();

    /**
     * The required amount to prove consistency.
     */
    private int requiredSignatures;

    /**
     * The decision of the message.
     */
    private String decision;

    /**
     * Is the signatureStorage processed by the holding replica.
     */
    private boolean isProcessed = false;

    /**
     * Checks if the storage has been sent to the clients already.
     */
    private boolean isDistributed = false;

    /**
     * Standard constructor for kryo.
     */
    public SignatureStorage()
    {
        /*
         * Standard constructor empty on purpose.
         */
    }

    /**
     * Public constructor to create the storage.
     * @param requiredSignatures the amount which are required.
     * @param message the message.
     * @param decision the decision.
     */
    public SignatureStorage(final int requiredSignatures, final byte[] message, final String decision)
    {
        this.requiredSignatures = requiredSignatures;
        this.message = message;
        this.decision = decision;
    }

    /**
     * Create a SignatureStorage out of another one.
     * (Copy constructor)
     * @param signatureStorage signatureStorage to use.
     */
    public SignatureStorage(final SignatureStorage signatureStorage)
    {
        this.requiredSignatures = signatureStorage.requiredSignatures;
        this.message = signatureStorage.message;
        this.decision = signatureStorage.decision;
        this.signatures.putAll(signatureStorage.signatures);
    }

    /**
     * Add a signature to the storage.
     * @param globalId the id of the server in the global cluster.
     * @param signature the signature to add.
     */
    public void addSignatures(final int globalId, final byte[] signature)
    {
        signatures.put(globalId, signature);
    }

    /**
     * Check if there are enough signatures stored to prove consistency.
     * @return true if the requiredAmount has been stored.
     */
    public boolean hasEnough()
    {
        return signatures.size() >= requiredSignatures;
    }

    /**
     * Getter for the message.
     * @return byte[] of the message.
     */
    public byte[] getMessage()
    {
        return this.message;
    }

    /**
     * Getter of the signatures.
     * @return the copy of the signatures list.
     */
    public Map<Integer, byte[]> getSignatures()
    {
        return new TreeMap<>(signatures);
    }

    /**
     * Getter for the decision.
     * @return the string of the decision.
     */
    public String getDecision()
    {
        return this.decision;
    }

    /**
     * Set the message of the signature storage.
     * @param message to set.
     */
    public void setMessage(final byte[] message)
    {
        this.message = message;
    }

    /**
     * Set that the replica holding this storage object processed the commit.
     */
    public void setProcessed()
    {
        this.isProcessed = true;
    }

    /**
     * Check if the signatureStorage has been processed by the owning replica.
     * @return true if so.
     */
    public boolean isProcessed()
    {
        return isProcessed;
    }

    /**
     * Check if storage has all signatures (Simulate this here with +1 for now)
     * @return true if s.
     */
    public boolean hasAll()
    {
        return signatures.size() >= requiredSignatures + 1;
    }

    /**
     * Set that the storage has been sent to the slave already.
     */
    public void setDistributed()
    {
        this.isDistributed = true;
    }

    /**
     * Check if the storage has been sent to the slave already.
     */
    public boolean isDistributed()
    {
        return this.isDistributed;
    }
}
