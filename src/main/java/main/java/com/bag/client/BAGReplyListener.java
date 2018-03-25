package main.java.com.bag.client;

import bftsmart.communication.client.ReplyListener;
import bftsmart.tom.RequestContext;
import bftsmart.tom.core.messages.TOMMessage;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;
import main.java.com.bag.util.ReadModes;

public class BAGReplyListener implements ReplyListener
{
    /**
     * The hooked up client using this.
     */
    final TestClient testClient;

    /**
     * The amount of required answers we need.
     */
    final int requiredResults;

    /**
     * The amount of results we received already.
     */
    int resultsReceived = 0;

    /**
     * The result (commit or abort)
     */
    int globalResult = -1;

    public BAGReplyListener(final TestClient testClient, final ReadModes readMode)
    {
        this.testClient = testClient;
        switch(readMode)
        {
            case TO_1_OTHER:
                requiredResults = 1;
                break;
            case PESSIMISTIC:
                requiredResults = 3;
                break;
            case LOCALLY_UNORDERED:
            case GLOBALLY_UNORDERED:
            case TO_F_PLUS_1_LOCALLY:
            case TO_F_PLUS_1_GLOBALLY:
            default:
                requiredResults = 2;
        }
    }

    @Override
    public void reset()
    {
        resultsReceived = 0;
        globalResult = -1;
    }

    @Override
    public void replyReceived(final RequestContext requestContext, final TOMMessage tomMessage)
    {
        final byte[] answer = tomMessage.getContent();
        final KryoPool pool = new KryoPool.Builder(testClient.factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        final Input input = new Input(answer);
        final String messageType = kryo.readObject(input, String.class);

        if (!Constants.COMMIT_RESPONSE.equals(messageType))
        {
            Log.getLogger().warn("Incorrect response type to client from server! ReqId: " + tomMessage.destination + "type: " + messageType + " ");
            testClient.resetSets();
            testClient.setFirstRead(true);
            pool.release(kryo);
            return;
        }

        final boolean commit = Constants.COMMIT.equals(kryo.readObject(input, String.class));
        final int result = commit ? 1 : 0;
        if(globalResult == -1 || globalResult == result)
        {
            resultsReceived++;
            Log.getLogger().info("Received messages: " + resultsReceived);
        }
        else
        {
            Log.getLogger().warn("Two different responses to client from servers! ReqId: " + tomMessage.destination);
            testClient.resetSets();
            testClient.setFirstRead(true);
            pool.release(kryo);
            return;
        }

        Log.getLogger().info("Going for commit: " + resultsReceived);
        if(resultsReceived == requiredResults)
        {
            if (commit)
            {
                globalResult = result;
                testClient.setLocalTimestamp(kryo.readObject(input, Long.class));
                testClient.resetSets();
                testClient.setFirstRead(true);
                pool.release(kryo);
                Log.getLogger().info(String.format("Transaction with local transaction id: %d successfully committed", testClient.getLocalTimestamp()));
                return;
            }
            Log.getLogger().info(String.format("Transaction with local transaction id: %d successfully aborted", testClient.getLocalTimestamp()));

            globalResult = result;
            testClient.resetSets();
        }
        Log.getLogger().info("Only: " + resultsReceived + " received");
        pool.release(kryo);
        return;
    }
}
