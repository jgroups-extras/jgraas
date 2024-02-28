package org.jgroups.jgraas.client;

import org.jgroups.*;
import org.jgroups.annotations.Component;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.cs.Receiver;
import org.jgroups.conf.AttributeType;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.jgraas.common.*;
import org.jgroups.jgraas.server.JChannelServer;
import org.jgroups.jgraas.util.Utils;
import org.jgroups.stack.Protocol;
import org.jgroups.util.ByteArray;
import org.jgroups.util.*;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link org.jgroups.JChannel} forwarding requests to a remote {@link JChannelServer}.
 * {@link ClientStubManager} is used to forward requests to the server, and translate received messages from the server
 * process
 * @author Bela Ban
 * @since  5.3.3
 */
@MBean(description="JChannel stub forwarding requests to a remote JGraaS server")
public class JChannelStub extends JChannel implements Receiver {

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

    @ManagedAttribute(description="The physical address of the remote server (cached on connect())")
    protected Address physical_addr;

    protected TimeScheduler              timer;
    // list of 1 server, when upgrading, a second server might be present
    protected List<InetSocketAddress>    servers=new ArrayList<>();
    protected SocketFactory              socket_factory;
    protected ClientStubManager          stub_mgr;
    protected final MessageFactory       msg_factory=new DefaultMessageFactory();
    protected Marshaller                 marshaller;
    protected Promise<ProtoJoinResponse> join_rsp=new Promise<>();

    public long         getReconnectInterval()       {return reconnect_interval;}
    public JChannelStub setReconnectInterval(long r) {this.reconnect_interval=r; return this;}
    public boolean      isTcpNodelay()               {return tcp_nodelay;}
    public JChannelStub setTcpNodelay(boolean nd)    {this.tcp_nodelay=nd;return this;}
    public boolean      useNio()                     {return use_nio;}
    public JChannelStub useNio(boolean use_nio)      {this.use_nio=use_nio; return this;}
    public int          getPortRange()               {return port_range;}
    public JChannelStub setPortRange(int r)          {port_range=r; return this;}
    public TLS          tls()                        {return tls;}
    public JChannelStub tls(TLS t)                   {this.tls=t; return this;}
    public int          getLinger()                  {return linger;}
    public JChannelStub setLinger(int l)             {this.linger=l; return this;}
    public boolean      nonBlockingSends()           {return non_blocking_sends;}
    public JChannelStub nonBlockingSends(boolean b)  {this.non_blocking_sends=b; return this;}
    public int          maxSendQueue()               {return max_send_queue;}
    public JChannelStub maxSendQueue(int s)          {this.max_send_queue=s; return this;}
    public Marshaller   marshaller()                 {return marshaller;}
    public JChannelStub marshaller(Marshaller m)     {this.marshaller=m; return this;}
    public JChannelStub timer(TimeScheduler t)       {this.timer=t; return this;}
    public Address      physicalAddress()            {return physical_addr;}


    public JChannelStub(boolean ignored) throws Exception {
        notImplemented("JChannelStub(boolean)");
    }

    public JChannelStub() throws Exception {
    }

    public JChannelStub(String ignored) throws Exception {
        notImplemented("JChannelStub(String)");
    }

    public JChannelStub(InputStream ignored) throws Exception {
        notImplemented("JChannelStub(InputStream)");
    }

    public JChannelStub(ProtocolStackConfigurator ignored) throws Exception {
        notImplemented("JChannelStub(ProtocolStackConfigurator)");
    }

    public JChannelStub(Protocol... ignored) throws Exception {
        notImplemented("JChannelStub(Protocol...)");
    }

    public JChannelStub(List<Protocol> ignored) throws Exception {
        notImplemented("JChannelStub(List<Protocol>)");
    }

    public JChannelStub(InetSocketAddress srv) throws Exception {
        addServer(srv);
    }

    public JChannelStub addServer(InetAddress addr, int port) {
        servers.add(new InetSocketAddress(addr, port));
        return this;
    }

    public JChannelStub addServer(InetSocketAddress addr) {
        servers.add(addr);
        return this;
    }

    public JChannelStub clearServerList() {
        servers.clear();
        return this;
    }

    /** Creates the connection to the remote server */
    public JChannelStub connectToRemote() throws Exception {
        if(timer == null) {
            ThreadFactory thread_factory=new DefaultThreadFactory("client-stub", true);
            timer=new TimeScheduler3(thread_factory, 1, 5, 30000, 200, "abort");
        }
        if(tls.enabled())
            socket_factory=tls.createSocketFactory();
        else
            socket_factory=new DefaultSocketFactory();
        if(server_list != null) {
            try {
                servers.addAll(Util.parseCommaDelimitedHosts2(server_list, port_range));
            }
            catch(Exception ex) {
                throw new IllegalArgumentException(ex);
            }
        }
        stub_mgr=new ClientStubManager(log, timer, reconnect_interval)
          .useNio(this.use_nio).socketFactory(socket_factory).heartbeat(heartbeat_interval, heartbeat_timeout)
          .nonBlockingSends(non_blocking_sends).maxSendQueue(max_send_queue);
        for(InetSocketAddress srv: servers) {
            InetSocketAddress target=null;
            try {
                target=srv.isUnresolved()? new InetSocketAddress(srv.getHostString(), srv.getPort()) : srv;
                stub_mgr.createAndRegisterStub(new InetSocketAddress((InetAddress)null, 0), target, linger)
                  .receiver(this).tcpNoDelay(tcp_nodelay);
            }
            catch(Throwable t) {
                log.error("%s: failed creating stub to %s: %s", target, t);
            }
        }
        stub_mgr.connectStubs();
        return this;
    }

    @Override
    public Object down(Event evt) {
        if(Event.GET_PHYSICAL_ADDRESS == evt.type())
            return physical_addr;
        return notImplemented("down(Event)");
    }

    @Override
    public Object down(Message msg) {
        return super.down(msg);
    }

    @Override
    protected synchronized JChannel connect(String cluster_name, boolean useFlushIfPresent) throws Exception {
        return connect(cluster_name);
    }

    @Override
    public synchronized JChannel connect(String cluster_name, Address target, long timeout) throws Exception {
        return (JChannel)notImplemented("connect() with state transfer");
    }

    @Override
    public synchronized JChannel connect(String cluster_name, Address target, long timeout, boolean useFlushIfPresent) throws Exception {
        return (JChannel)notImplemented("connect() with state transfer");
    }

    public JChannelStub connect(String cluster) throws Exception {
        ProtoJoinRequest.Builder builder=ProtoJoinRequest.newBuilder().setClusterName(cluster);
        if(name != null)
            builder.setName(name);
        ProtoRequest req=ProtoRequest.newBuilder().setJoinReq(builder.build()).build();
        join_rsp.reset(true);
        send(req);
        ProtoJoinResponse rsp=join_rsp.getResultWithTimeout(3000, true);
        if(rsp.hasLocalAddress())
            this.local_addr=Utils.protoAddressToJGAddress(rsp.getLocalAddress());
        this.name=rsp.getName();
        this.cluster_name=rsp.getCluster();
        this.physical_addr=Utils.protoIpAddressToJG(rsp.getIpAddr());
        state=State.CONNECTED;
        notifyChannelConnected(this);
        return this;
    }

    public JChannelStub disconnect() {
        ProtoRequest req=ProtoRequest.newBuilder().setLeaveReq(ProtoLeaveRequest.newBuilder()).build();
        try {
            send(req);
        }
        catch(Exception ex) {
            log.error("%s: disconnect failed: %s", local_addr, ex);
        }
        return this;
    }

    public void close() {
        super.close();
        if(stub_mgr != null)
            stub_mgr.destroyStubs();
    }

    @Override
    public JChannelStub send(Message msg) throws Exception {
        ProtoMessage m=Utils.jgMessageToProto(cluster_name, msg, null);
        ProtoRequest req=ProtoRequest.newBuilder().setMessage(m).build();
        return send(req);
    }

    protected JChannelStub send(ProtoRequest req) throws Exception {
        ClientStub primary=stub_mgr.getPrimary();
        if(primary != null) {
            ByteArray buf=Utils.serialize(req);
            primary.send(buf);
            return this;
        }
        else
            throw new IllegalStateException(String.format("%s: send failed (not connected to primary)", local_addr));
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

    // received from the server
    protected void handleRequest(ProtoRequest req) throws Exception {
        ProtoRequest.ChoiceCase c=req.getChoiceCase();
        switch(c) {
            case MESSAGE:
                Message msg=Utils.protoMessageToJG(req.getMessage(), msg_factory, marshaller);
                up(msg);
                break;
            case MESSAGE_BATCH:
                MessageBatch batch=Utils.protoMessageBatchToJG(req.getMessageBatch(), msg_factory, marshaller);
                up(batch);
                break;
            case JOIN_REQ:
                throw new IllegalStateException("join request not handled by client");
            case JOIN_RSP:
                join_rsp.setResult(req.getJoinRsp());
                break;
            case LEAVE_REQ:
                throw new IllegalStateException("leave request not handled by client");
            case VIEW:
                View tmp=Utils.protoViewToJGView(req.getView());
                up(new Event(Event.VIEW_CHANGE, tmp));
                break;
            case EXCEPTION:
                break;
            default:
                log.warn("request %s not known", c);
                break;
        }
    }

    protected static Object notImplemented(String method) {
        // log.warn("method %s is not implemented in %s", method, JChannelStub.class.getSimpleName());
        String message=String.format("method %s is not implemented in %s", method, JChannelStub.class.getSimpleName());
        throw new IllegalArgumentException(message);
    }

}
