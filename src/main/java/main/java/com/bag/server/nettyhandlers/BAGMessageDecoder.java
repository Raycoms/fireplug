package main.java.com.bag.server.nettyhandlers;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

/**
 * Message decoder for all netty messages for the direct access client/server communication.
 */
public class BAGMessageDecoder extends ByteToMessageDecoder
{

    @Override
    protected void decode(final ChannelHandlerContext ctx, final ByteBuf in, final List<Object> out) throws Exception
    {
        if (in.readableBytes() < 4)
        {
            return;
        }
        in.markReaderIndex();
        final int length = in.readInt();
        if (in.readableBytes() < length)
        {
            in.resetReaderIndex();
            return;
        }

        final BAGMessage message = new BAGMessage();
        message.size = length;
        message.buffer = new byte[length];
        in.readBytes(message.buffer);
        out.add(message);
    }
}
