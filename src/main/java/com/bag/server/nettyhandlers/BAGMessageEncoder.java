package com.bag.server.nettyhandlers;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Message encoder for all netty messages for the direct access client/server communication.
 */
public class BAGMessageEncoder extends MessageToByteEncoder<BAGMessage>
{
    @Override
    protected void encode(final ChannelHandlerContext ctx, final BAGMessage msg, final ByteBuf out) throws Exception
    {
        out.writeInt(msg.size);
        out.writeBytes(msg.buffer);
    }
}
