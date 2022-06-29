package membership.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.tuple.Pair;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class PushMessage extends ProtoMessage {

    public final static short MSG_ID = 101;

    private final List<Pair<Host, Integer>> buffer;

    public PushMessage(List<Pair<Host, Integer>> buffer) {
        super(MSG_ID);
        this.buffer = buffer;
    }

    public List<Pair<Host, Integer>> getBuffer() {
        return buffer;
    }

    @Override
    public String toString() {
        return "PushMessage{" +
                "buffer=" + buffer +
                '}';
    }

    public static ISerializer<PushMessage> serializer = new ISerializer<PushMessage>() {
        @Override
        public void serialize(PushMessage pushMessage, ByteBuf out) throws IOException {
            out.writeInt(pushMessage.buffer.size());
            for (Pair<Host, Integer> pair : pushMessage.buffer) {
                Host.serializer.serialize(pair.getKey(), out);
                out.writeInt(pair.getValue());
            }
        }

        @Override
        public PushMessage deserialize(ByteBuf in) throws IOException {
            int size = in.readInt();
            List<Pair<Host, Integer>> buffer = new LinkedList<>();
            for (int i = 0; i < size; i++) {
                Host h = Host.serializer.deserialize(in);
                int age = in.readInt();
                buffer.add(Pair.of(h, age));
            }
            return new PushMessage(buffer);
        }
    };
}
