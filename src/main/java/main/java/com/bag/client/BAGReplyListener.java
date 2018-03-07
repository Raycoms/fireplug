package main.java.com.bag.client;

import bftsmart.communication.client.ReplyListener;
import bftsmart.tom.RequestContext;
import bftsmart.tom.core.messages.TOMMessage;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.pool.KryoPool;
import main.java.com.bag.util.Constants;
import main.java.com.bag.util.Log;

public class BAGReplyListener implements ReplyListener
{
    final TestClient testClient;
    int resultsReceived = 0;
    int globalResult = -1;

    public BAGReplyListener(final TestClient testClient)
    {
        this.testClient = testClient;
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
        final byte[] answer = requestContext.getRequest();
        final KryoPool pool = new KryoPool.Builder(testClient.factory).softReferences().build();
        final Kryo kryo = pool.borrow();

        final Input input = new Input(answer);
        final String messageType = kryo.readObject(input, String.class);

        if (!Constants.COMMIT_RESPONSE.equals(messageType))
        {
            Log.getLogger().warn("Incorrect response type to client from server! ReqId: " + requestContext.getReqId() + "type: " + messageType + " " + Constants.COMMIT.equals(kryo.readObject(input, String.class)) + " ");
            testClient.resetSets();
            testClient.setFirstRead(true);
            pool.release(kryo);
            return;
        }

        final boolean commit = Constants.COMMIT.equals(kryo.readObject(input, String.class));
        final int result = commit ? 1 : 0;
        if(globalResult != -1 && globalResult ==result)
        {
            resultsReceived++;
        }
        else
        {
            Log.getLogger().warn("Two different responses to client from servers! ReqId: " + requestContext.getReqId());
            testClient.resetSets();
            testClient.setFirstRead(true);
            pool.release(kryo);
            return;
        }

        if(resultsReceived == 2)
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
            globalResult = result;
            testClient.resetSets();
        }
        pool.release(kryo);
        return;
    }
}
