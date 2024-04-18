package org.jgroups.jgraas.demos;

import org.jgroups.JChannel;
import org.jgroups.blocks.ReplicatedHashMap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Bela Ban
 * @since  5.3.5
 */
public class JGraasHashMap extends ReplicatedHashMap<String,String> {
    public JGraasHashMap(JChannel channel) {
        super(channel);
    }

    public JGraasHashMap(ConcurrentMap<String,String> map, JChannel channel) {
        super(map, channel);
    }

    @Override
    public void getState(OutputStream ostream) throws Exception {
        Map<String,String> copy=new HashMap<>(map);
        try(DataOutputStream out=new DataOutputStream(ostream)) {
            out.writeInt(copy.size());
            for(Map.Entry<String,String> e: copy.entrySet()) {
                out.writeUTF(e.getKey());
                out.writeUTF(e.getValue());
            }
        }
    }

    @Override
    public void setState(InputStream istream) throws Exception {
        try(DataInputStream in=new DataInputStream(istream)) {
            int size=in.readInt();
            if(size == 0)
                return;
            for(int i=0; i < size; i++) {
                String key=in.readUTF(), val=in.readUTF();
                map.put(key, val);
            }
        }
    }
}
