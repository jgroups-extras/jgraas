package org.jgroups.jgraas.client;

import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Demo client using {@link JChannelStub} to connect to a remote {@link org.jgroups.jgraas.server.JChannelServer}
 * @author Bela Ban
 * @since  5.3.3
 */
public class Client implements Receiver, Closeable {
    protected JChannelStub stub;

    public void start(InetSocketAddress srv, String cluster, String name) throws Exception {
        stub=new JChannelStub(srv).setPortRange(0);
        if(name != null)
            stub.setName(name);
        stub.setReceiver(this);
        stub.connectToRemote();
        stub.connect(cluster);

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
                Message msg=new ObjectMessage(null, line);
                stub.send(msg);
            }
            catch(IOException | IllegalArgumentException io_ex) {
                break;
            }
            catch(Exception ex) {
                ex.printStackTrace();
            }
        }
        stub.close();
    }

    @Override
    public void close() throws IOException {
        Util.close(stub);
    }

    @Override
    public void receive(Message msg) {
        System.out.printf("-- msg from %s: %s\n", msg.src(), msg.getObject());
    }

    @Override
    public void receive(MessageBatch batch) {
        int index=1;
        for(Message m: batch) {
            Object o=m.getPayload();
            System.out.printf("-- msg %d from %s: %s\n", index++, m.getSrc(), o);
        }
    }

    @Override
    public void viewAccepted(View new_view) {
        System.out.printf("-- view: %s\n", new_view);
    }

    public static void main(String[] args) throws Exception {
        InetAddress server_addr=InetAddress.getLocalHost();
        int port=12500;
        String cluster="cluster", name=null;

        for(int i=0; i < args.length; i++) {
            if("-server".equals(args[i])) {
                server_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if("-port".equals(args[i])) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if("-cluster".equals(args[i])) {
                cluster=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            System.out.printf("%s [-server <address>] [-port <port>] [-cluster name] [-name <node name>\n",
                              JChannelStub.class.getSimpleName());
            return;
        }

        Client client=new Client();
        InetSocketAddress target=new InetSocketAddress(server_addr, port);
        try {
            client.start(target, cluster, name);
        }
        catch(Exception ex) {
            System.out.printf("Failed connecting to server %s: %s\n", target, ex);
            Util.close(client);
        }
    }
}
