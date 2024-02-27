package org.jgroups.jgraas.client;

import org.jgroups.ObjectMessage;
import org.jgroups.jgraas.common.ProtoMessage;
import org.jgroups.jgraas.common.ProtoRequest;
import org.jgroups.jgraas.util.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;

/**
 * Demo client using {@link JChannelStub} to connect to a remote {@link org.jgroups.jgraas.server.JChannelServer}
 * @author Bela Ban
 * @since  5.3.3
 */
public class Client {
    public static void main(String[] args) throws Exception {
        InetAddress server_addr=InetAddress.getLocalHost();
        int port=12500;

        for(int i=0; i < args.length; i++) {
            if("-server".equals(args[i])) {
                server_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if("-port".equals(args[i])) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.printf("%s [-server <address>] [-port <port>]\n", JChannelStub.class.getSimpleName());
            return;
        }

        JChannelStub client=new JChannelStub().setPortRange(0).addServer(server_addr, port);
        client.init();
        client.start();
        client.connect("cluster");

        BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            try {
                System.out.print("> "); System.out.flush();
                String line=in.readLine();
                line=line != null? line.toLowerCase() : null;
                if(line == null)
                    continue;
                if(line.startsWith("quit") || line.startsWith("exit"))
                    break;
                org.jgroups.Message msg=new ObjectMessage(null, line);
                ProtoMessage m=Utils.jgMessageToProto("cluster", msg, null);
                ProtoRequest req=ProtoRequest.newBuilder().setMessage(m).build();
                client.send(req);
            }
            catch(IOException | IllegalArgumentException io_ex) {
                break;
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }
        }
        client.disconnect("cluster");
        client.destroy();
    }
}
