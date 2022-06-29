package network;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

public class ResponseDecoder extends ReplayingDecoder<ResponseDecoder.ResponseDecoderState> {

  private int cId;
  private int respSize;

  public ResponseDecoder(){
    super(ResponseDecoderState.READ_CID);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    switch (state()) {
      case READ_CID:
        cId = in.readInt();
        checkpoint(ResponseDecoderState.READ_RESP_SIZE);
      case READ_RESP_SIZE:
        respSize = in.readInt();
        checkpoint(ResponseDecoderState.READ_RESP);
      case READ_RESP:
        byte[] respBytes = new byte[respSize];
        if(respSize > 0)
          in.readBytes(respBytes);
        checkpoint(ResponseDecoderState.READ_CID);
        out.add(new ResponseMessage(cId, respBytes));
        break;
      default:
        throw new Error("Shouldn't reach here.");
    }
  }

  public enum ResponseDecoderState {
    READ_CID,
    READ_RESP_SIZE,
    READ_RESP
  }

}
