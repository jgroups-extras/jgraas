package org.jgroups.jgraas.client;

import org.jgroups.*;
import org.jgroups.annotations.Component;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.cs.Receiver;
import org.jgroups.conf.AttributeType;
import org.jgroups.jgraas.common.*;
import org.jgroups.jgraas.server.JChannelServer;
import org.jgroups.jgraas.util.Utils;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.*;
import org.jgroups.util.ByteArray;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link org.jgroups.JChannel} forwarding requests to a remote
 * {@link JChannelServer}. {@link ClientStubManager} is used to forward requests to the server, and
 * translate receive requests from the server.
 * process
 * @author Bela Ban
 * @since  5.3.3
 */
@MBean(description="JChannel stub forwarding requests to a remote JGraaS server")
public class JChannelClient implements Receiver {

    @Property(description = "Interval in msec to attempt connecting back to router in case of closed connection",
      type= AttributeType.TIME)
    protected long    reconnect_interval=5000;

    @Property(description="Should TCP no delay flag be turned on")
    protected boolean tcp_nodelay;

    @Property(description="Whether to use blocking (false) or non-blocking (true) connections")
    protected boolean use_nio;

    @Property(description="A comma-separated list of servers, e.g. HostA[12500],HostB[12500]")
    protected String  server_list;

    @Property(description="The range of valid ports: [bind_port .. bind_port+port_range ]. " +
      "0 only binds to bind_port and fails if taken")
    protected int     port_range=10; // 27-6-2003 bgooren, Only try one port by default

    @Property(description="Sends a heartbeat to the server every heartbeat_interval ms (0 disables this)",
      type=AttributeType.TIME)
    protected long    heartbeat_interval;

    @Property(description="Max time (ms) with no received message or heartbeat after which the connection to a " +
      "Server is closed. Ignored when heartbeat_interval is 0.", type=AttributeType.TIME)
    protected long    heartbeat_timeout;

    @Property(description="SO_LINGER in seconds. Default of -1 disables it")
    protected int     linger=-1; // SO_LINGER (number of seconds, -1 disables it)

    @Property(description="use bounded queues for sending (https://issues.redhat.com/browse/JGRP-2759)")
    protected boolean non_blocking_sends;

    @Property(description="when sending and non_blocking, how many messages to queue max")
    protected int     max_send_queue=128;

    @Component(name="tls",description="Contains the attributes for TLS (SSL sockets) when enabled=true")
    protected TLS     tls=new TLS();

    protected final Log               log=LogFactory.getLog(getClass());
    protected TimeScheduler           timer;
    protected List<InetSocketAddress> servers=new ArrayList<>();
    protected SocketFactory           socket_factory;
    protected ClientStubManager       stub_mgr;
    protected final MessageFactory    msg_factory=new DefaultMessageFactory();
    protected Marshaller              marshaller;

    public long           getReconnectInterval()       {return reconnect_interval;}
    public JChannelClient setReconnectInterval(long r) {this.reconnect_interval=r; return this;}
    public boolean        isTcpNodelay()               {return tcp_nodelay;}
    public JChannelClient setTcpNodelay(boolean nd)    {this.tcp_nodelay=nd;return this;}
    public boolean        useNio()                     {return use_nio;}
    public JChannelClient useNio(boolean use_nio)      {this.use_nio=use_nio; return this;}
    public int            getPortRange()               {return port_range;}
    public JChannelClient setPortRange(int r)          {port_range=r; return this;}
    public TLS            tls()                        {return tls;}
    public JChannelClient tls(TLS t)                   {this.tls=t; return this;}
    public int            getLinger()                  {return linger;}
    public JChannelClient setLinger(int l)             {this.linger=l; return this;}
    public boolean        nonBlockingSends()           {return non_blocking_sends;}
    public JChannelClient nonBlockingSends(boolean b)  {this.non_blocking_sends=b; return this;}
    public int            maxSendQueue()               {return max_send_queue;}
    public JChannelClient maxSendQueue(int s)          {this.max_send_queue=s; return this;}
    public Marshaller     marshaller()                 {return marshaller;}
    public JChannelClient marshaller(Marshaller m)     {this.marshaller=m; return this;}

    public JChannelClient() {
        this(null);
    }

    public JChannelClient(TimeScheduler timer) {
        this.timer=timer;
    }

    public JChannelClient addServer(InetAddress addr, int port) {
        servers.add(new InetSocketAddress(addr, port));
        return this;
    }

    public void init() throws Exception {
        if(timer == null) {
            ThreadFactory thread_factory=new DefaultThreadFactory("client-stub", true);
            timer=new TimeScheduler3(thread_factory, 1, 5, 30000, 200, "abort");
        }
        if(tls.enabled())
            socket_factory=tls.createSocketFactory();
        else
            socket_factory=new DefaultSocketFactory();
        if(server_list != null)
            servers.addAll(Util.parseCommaDelimitedHosts2(server_list, port_range));
        stub_mgr=ClientStubManager.emptyClientStubManager(log, timer).useNio(this.use_nio)
          .nonBlockingSends(non_blocking_sends).maxSendQueue(max_send_queue);
    }

    public void start() throws Exception {
     //    super.start();

        stub_mgr=new ClientStubManager(log, timer, reconnect_interval)
          .useNio(this.use_nio).socketFactory(socket_factory).heartbeat(heartbeat_interval, heartbeat_timeout)
          .nonBlockingSends(non_blocking_sends).maxSendQueue(max_send_queue);
        for(InetSocketAddress srv: servers) {
            InetSocketAddress target=null;
            try {
                target=srv.isUnresolved()? new InetSocketAddress(srv.getHostString(), srv.getPort())
                  : new InetSocketAddress(srv.getAddress(), srv.getPort());
                stub_mgr.createAndRegisterStub(new InetSocketAddress((InetAddress)null, 0), target, linger)
                  .receiver(this).tcpNoDelay(tcp_nodelay);
            }
            catch(Throwable t) {
                log.error("%s: failed creating stub to %s: %s", target, t);
            }
        }
        stub_mgr.connectStubs();
    }

    public void destroy() {
        if(stub_mgr != null)
            stub_mgr.destroyStubs();
        // super.destroy();
    }

    public void connect(String cluster) throws IOException {
        ProtoRequest req=ProtoRequest.newBuilder().setJoinReq(ProtoJoinRequest.newBuilder().setClusterName(cluster)).build();
        send(req);
    }

    public void disconnect(String cluster) throws IOException {
        ProtoRequest req=ProtoRequest.newBuilder().setLeaveReq(ProtoLeaveRequest.newBuilder().setClusterName(cluster)).build();
        send(req);
    }

    protected void send(ProtoRequest req) throws IOException {
        ByteArray buf=Utils.serialize(req);
        stub_mgr.forAny(st -> {
            try {
                st.send(buf);
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        });
    }


    public void send(ByteArray buf) { // todo: only for testing; remove again!
        stub_mgr.forAny(st -> {
            try {
                st.send(buf);
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override public void receive(Address sender, byte[] buf, int offset, int length) {
        receive(sender, ByteBuffer.wrap(buf, offset, length));
    }

    @Override
    public void receive(Address sender, ByteBuffer buf) {
        try {
            ByteArrayInputStream in=new ByteArrayInputStream(buf.array(), buf.arrayOffset(), buf.remaining());
            handleRequest(ProtoRequest.parseFrom(in));
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void receive(Address sender, DataInput in, int length) throws Exception {
        try {
            if(in instanceof InputStream) {
                handleRequest(ProtoRequest.parseDelimitedFrom((InputStream)in));
            }
            else {
                byte[] buf=new byte[length];
                in.readFully(buf);
                ByteArrayInputStream input=new ByteArrayInputStream(buf);
                handleRequest(ProtoRequest.parseDelimitedFrom(input));
            }
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void handleRequest(ProtoRequest req) throws Exception {
        ProtoRequest.ChoiceCase c=req.getChoiceCase();
        switch(c) {
            case MESSAGE:
                Message msg=Utils.protoMessageToJG(req.getMessage(), msg_factory, marshaller);
                Object obj=msg.getPayload();
                System.out.printf("-- msg from %s: %s\n", msg.getSrc(), obj);
                break;
            case MESSAGE_BATCH:
                MessageBatch batch=Utils.protoMessageBatchToJG(req.getMessageBatch(), msg_factory, marshaller);
                int index=1;
                for(Message m: batch) {
                    Object o=m.getPayload();
                    System.out.printf("-- msg %d from %s: %s\n", index++, m.getSrc(), o);
                }
                break;
            case JOIN_REQ:
            case LEAVE_REQ:
                break;
            case VIEW:
                org.jgroups.View view=Utils.protoViewToJGView(req.getView());
                System.out.printf("-- view: %s\n", view);
                break;
            case EXCEPTION:
                break;
            default:
                log.warn("request %s not known", c);
                break;
        }
    }

    protected void disconnectStub() {
        stub_mgr.disconnectStubs();
    }

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
            System.out.printf("%s [-server <address>] [-port <port>]\n", JChannelClient.class.getSimpleName());
            return;
        }

        JChannelClient client=new JChannelClient().setPortRange(0).addServer(server_addr, port);
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
