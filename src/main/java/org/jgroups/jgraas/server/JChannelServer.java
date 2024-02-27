package org.jgroups.jgraas.server;

import org.jgroups.Receiver;
import org.jgroups.*;
import org.jgroups.annotations.Component;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.cs.*;
import org.jgroups.conf.AttributeType;
import org.jgroups.jgraas.common.*;
import org.jgroups.jgraas.util.Utils;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.jmx.ReflectUtils;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.DiagnosticsHandler;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ByteArray;
import org.jgroups.util.*;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.InputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * JGraaS server processing requests from remote clients to the local {@link org.jgroups.JChannel}.
 * @author Bela Ban
 * @since 5.3.3
 */
public class JChannelServer extends ReceiverAdapter implements ConnectionListener, DiagnosticsHandler.ProbeHandler {
    @ManagedAttribute(description="The bind address which should be used by the server")
    protected InetAddress          bind_addr;

    @ManagedAttribute(description="server port on which the server accepts client connections", writable=true)
    protected int                  port=12500;

    @ManagedAttribute(description="The cluster name")
    protected String               cluster_name;
    
    @ManagedAttribute(description="Time (in msecs) until idle client connections are closed. 0 disables expiration.",
      writable=true,type=AttributeType.TIME)
    protected long                 expiry_time;

    @ManagedAttribute(description="Interval (in msecs) to check for expired connections and close them. 0 disables reaping.",
      writable=true,type=AttributeType.TIME)
    protected long                 reaper_interval;

    @ManagedAttribute(description="Time (in ms) for setting SO_LINGER on sockets returned from accept(). 0 means do not set SO_LINGER"
      ,type=AttributeType.TIME)
    protected int                  linger_timeout=-1;

    protected ThreadFactory        thread_factory=new DefaultThreadFactory("jgraas-server", false, true);

    protected SocketFactory        socket_factory=new DefaultSocketFactory();

    @ManagedAttribute(description="Initial size of the TCP/NIO receive buffer (in bytes)")
    protected int                  recv_buf_size;

    @ManagedAttribute(description="Expose the server via JMX")
    protected boolean              jmx;

    @ManagedAttribute(description="Use non-blocking IO (true) or blocking IO (false). Cannot be changed at runtime")
    protected boolean              use_nio;

    @ManagedAttribute(description="The max number of bytes a message can have. If greater, an exception will be " +
      "thrown. 0 disables this", type=AttributeType.BYTES)
    protected int                  max_length;

    @Component(name="diag",description="DiagnosticsHandler listening for probe requests")
    protected DiagnosticsHandler   diag;
    @Component(name="tls",description="Contains the attributes for TLS (SSL sockets) when enabled=true")
    protected TLS                  tls=new TLS();

    @ManagedAttribute(description="Use bounded queues for sending (https://issues.redhat.com/browse/JGRP-2759)) in TCP (use_nio=false)")
    protected boolean              non_blocking_sends;

    @ManagedAttribute(description="When sending and non_blocking, how many messages to queue max")
    protected int                  max_send_queue=128;

    @Property(description="Configuration of the channel")
    protected String               config="udp.xml";

    @Property(description="The channel name")
    protected String               name;

    protected JChannel             channel;
    protected BaseServer           server;
    protected Marshaller           marshaller;
    protected JChannelReceiver     receiver=new JChannelReceiver();
    protected final MessageFactory msg_factory=new DefaultMessageFactory();
    protected final AtomicBoolean  running=new AtomicBoolean(false);
    protected final Log            log=LogFactory.getLog(this.getClass());




    public JChannelServer(String bind_addr, int local_port) throws Exception {
        this(bind_addr != null? InetAddress.getByName(bind_addr) : null, local_port, null);
    }

    public JChannelServer(InetAddress bind_addr, int local_port) throws Exception {
        this(bind_addr, local_port, null);
    }

    public JChannelServer(InetAddress bind_addr, int local_port, String config) throws Exception {
        this.port=local_port;
        this.bind_addr=bind_addr;
        init();
        if(config != null)
            this.config=config;
    }

    public String         config()                           {return config;}
    public JChannelServer config(String cfg)                 {this.config=cfg; return this;}
    public String         name()                             {return name;}
    public JChannelServer name(String n)                     {this.name=n; if(channel != null) channel.name(n); return this;}
    public Address        localAddress()                     {return server.localAddress();}
    public String         bindAddress()                      {return bind_addr != null? bind_addr.toString() : null;}
    public JChannelServer bindAddress(InetAddress addr)      {this.bind_addr=addr; return this;}
    public int            port()                             {return port;}
    public JChannelServer port(int port)                     {this.port=port; return this;}
    public long           expiryTime()                       {return expiry_time;}
    public JChannelServer expiryTime(long t)                 {this.expiry_time=t; return this;}
    public long           reaperInterval()                   {return reaper_interval;}
    public JChannelServer reaperInterval(long t)             {this.reaper_interval=t; return this;}
    public int            lingerTimeout()                    {return linger_timeout;}
    public JChannelServer lingerTimeout(int t)               {this.linger_timeout=t; return this;}
    public int            recvBufferSize()                   {return recv_buf_size;}
    public JChannelServer recvBufferSize(int s)              {recv_buf_size=s; return this;}
    public ThreadFactory  threadPoolFactory()                {return thread_factory;}
    public JChannelServer threadPoolFactory(ThreadFactory f) {this.thread_factory=f; return this;}
    public SocketFactory  socketFactory()                    {return socket_factory;}
    public JChannelServer socketFactory(SocketFactory sf)    {this.socket_factory=sf; return this;}
    public boolean        jmx()                              {return jmx;}
    public JChannelServer jmx(boolean flag)                  {jmx=flag; return this;}
    public boolean        useNio()                           {return use_nio;}
    public JChannelServer useNio(boolean flag)               {use_nio=flag; return this;}
    public int            maxLength()                        {return max_length;}
    public JChannelServer maxLength(int len)                 {max_length=len; if(server != null) server.setMaxLength(len);
                                                             return this;}
    public DiagnosticsHandler diagHandler()                  {return diag;}
    public TLS            tls()                              {return tls;}
    public JChannelServer tls(TLS t)                         {this.tls=t; return this;}
    public boolean        nonBlockingSends()                 {return non_blocking_sends;}
    public JChannelServer nonBlockingSends(boolean b)        {this.non_blocking_sends=b; return this;}
    public int            maxSendQueue()                     {return max_send_queue;}
    public JChannelServer maxSendQueue(int s)                {this.max_send_queue=s; return this;}
    public Marshaller     marshaller()                       {return marshaller;}
    public JChannelServer marshaller(Marshaller m)           {this.marshaller=m; return this;}


    @ManagedAttribute(description="operational status", name="running")
    public boolean running() {return running.get();}


    public JChannelServer init() throws Exception {
        diag=new DiagnosticsHandler(log, socket_factory, thread_factory).registerProbeHandler(this)
          .printHeaders(b -> String.format("JChannelServer [addr=%s, cluster=JChannelServer, version=%s]\n",
                                           localAddress(), Version.description));
        return this;
    }


    /**
     * Lifecycle operation. Called after create(). When this method is called, the managed attributes
     * have already been set.<br>
     * Brings the Router into a fully functional state.
     */
    @ManagedOperation(description="Lifecycle operation. Called after create(). When this method is called, "
            + "the managed attributes have already been set. Brings the server into a fully functional state.")
    public JChannelServer start() throws Exception {
        if(!running.compareAndSet(false, true))
            return this;

        channel=new JChannel(config).name(name).setReceiver(receiver);

        if(jmx)
            JmxConfigurator.register(this, Util.getMBeanServer(), "jgroups:name=JChannelServer");

        if(diag.isEnabled()) {
            StackType ip_version=bind_addr instanceof Inet6Address? StackType.IPv6 : StackType.IPv4;
            Configurator.setDefaultAddressValues(diag, ip_version);
            diag.start();
        }
        // Creating the DiagnosticsHandler _before_ the TLS socket factory makes the former use regular sockets;
        // if this was not the case, Probe would have to be extended to use SSLSockets, too
        if(tls.enabled()) {
            SocketFactory factory=tls.createSocketFactory();
            socketFactory(factory);
        }

        server=use_nio? new NioServer(thread_factory, socket_factory, bind_addr, port, port, null, 0,
                                      recv_buf_size, "jgroups.nio.gossiprouter")
          : new TcpServer(thread_factory, socket_factory, bind_addr, port, port, null, 0, recv_buf_size,
                          "jgroups.tcp.gossiprouter").nonBlockingSends(non_blocking_sends).maxSendQueue(max_send_queue);
        server.receiver(this).setMaxLength(max_length)
          .addConnectionListener(this)
          .connExpireTimeout(expiry_time).reaperInterval(reaper_interval).linger(linger_timeout);
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(JChannelServer.this::stop));
        return this;
    }


    /**
     * Always called before destroy(). Close connections and frees resources.
     */
    public void stop() {
        if(!running.compareAndSet(true, false))
            return;

        try {
            JmxConfigurator.unregister(this, Util.getMBeanServer(), "jgroups:name=JChannelServer");
        }
        catch(Exception ex) {
            log.error(Util.getMessage("MBeanDeRegistrationFailed"), ex);
        }
        Util.close(diag, server, channel);
        log.debug("router stopped");
    }


    @ManagedOperation(description="Prints all connections")
    public String printConnections() {
        return server.printConnections();
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

    /** Handles requests received from remote client: apply to channel (e.g. send message */
    protected void handleRequest(ProtoRequest req) throws Exception {
        ProtoRequest.ChoiceCase c=req.getChoiceCase();
        switch(c) {
            case MESSAGE:
                Message msg=Utils.protoMessageToJG(req.getMessage(), msg_factory, marshaller);
                Object obj=msg.getPayload();
                System.out.printf("-- msg from %s: %s\n", msg.getSrc(), obj);
                channel.send(msg);
                break;
            case MESSAGE_BATCH:
                throw new IllegalStateException("server cannot receive message batches");
            case JOIN_REQ:
                cluster_name=req.getJoinReq().getClusterName();
                channel.connect(cluster_name);
                break;
            case LEAVE_REQ:
                channel.disconnect();
                break;
            case VIEW:
                throw new IllegalStateException("server cannot receive views");
            case EXCEPTION:
                break;
            default:
                log.warn("request %s not known", c);
                break;
        }
    }


    public Map<String,String> handleProbe(String... keys) {
        Map<String,String> map=new TreeMap<>();
        for(String key: keys) {
            if(key.startsWith("ops")) {
                map.put(key, ReflectUtils.listOperations(getClass()));
                continue;
            }
            if(key.startsWith("op") || key.startsWith("invoke")) {
                int index=key.indexOf('=');
                if(index == -1)
                    throw new IllegalArgumentException(String.format("\\= not found in operation %s", key));
                String op=key.substring(index + 1).trim();
                try {
                    ReflectUtils.invokeOperation(map, op, this);
                }
                catch(Exception ex) {
                    throw new IllegalArgumentException(String.format("operation %s failed: %s", op, ex));
                }
                continue;
            }
            if(key.startsWith("jmx")) {
                int index=key.indexOf('=');
                String tmp;
                if(index == -1)
                    tmp=null;
                else
                    tmp=key.substring(index+1);
                ReflectUtils.handleAttributeAccess(map, tmp, this);
                continue;
            }
            if(key.startsWith("keys")) {
                StringBuilder sb=new StringBuilder();
                for(DiagnosticsHandler.ProbeHandler handler : diag.getProbeHandlers()) {
                    String[] tmp=handler.supportedKeys();
                    if(tmp != null) {
                        for(String s: tmp)
                            sb.append(s).append(" ");
                    }
                }
                map.put(key, sb.toString());
                continue;
            }
            if(key.startsWith("member-addrs")) {
                InetSocketAddress sa=(InetSocketAddress)diag.getLocalAddress();
                if(sa != null) {
                    Set<PhysicalAddress> physical_addrs=Set.of(new IpAddress(sa.getAddress(), sa.getPort()));
                    // Set<PhysicalAddress> physical_addrs=diag.getLocalAddress();
                    String list=Util.print(physical_addrs);
                    map.put(key, list);
                }
                continue;
            }
            if(key.startsWith("dump")) {
                map.put(key, Util.dumpThreads());
            }
        }
        return map;
    }

    public String[] supportedKeys() {
        return new String[]{"ops", "op", "invoke", "keys", "member-addrs", "dump"};
    }


    @Override public void connectionEstablished(Connection conn) {
        log.debug("connection to %s established", conn.peerAddress());
    }

    @Override public void connectionClosed(Connection conn) {}


    protected void send(ProtoRequest req) throws Exception {
        ByteArray buf=Utils.serialize(req);
        server.send(null, buf.getArray(), buf.getOffset(), buf.getLength());
    }

    /** Receives messages from other members: forward to remote client */
    protected class JChannelReceiver implements Receiver {

        @Override
        public void receive(Message msg) {
            try {
                System.out.printf("-- received msg from %s: %s\n", msg.src(), msg.getPayload());
                ProtoMessage m=Utils.jgMessageToProto(cluster_name, msg, marshaller);
                ProtoRequest req=ProtoRequest.newBuilder().setMessage(m).build();
                send(req);
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void receive(MessageBatch batch) {
            System.out.printf("-- received msg batch from %s: %d msgs\n", batch.sender(), batch.size());
            ProtoMessageBatch mb=Utils.jgMessageBatchToProto(batch, marshaller);
            ProtoRequest req=ProtoRequest.newBuilder().setMessageBatch(mb).build();
            try {
                send(req);
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void viewAccepted(View new_view) {
            ProtoView pv=Utils.jgViewToProtoView(new_view);
            ProtoRequest req=ProtoRequest.newBuilder().setView(pv).build();
            try {
                send(req);
            }
            catch(Exception e) {
                log.warn("failed serializing/sending request", e);
            }
        }
    }

}
