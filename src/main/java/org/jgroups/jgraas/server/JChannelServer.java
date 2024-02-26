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
import org.jgroups.util.*;
import org.jgroups.util.ByteArray;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.InputStream;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.util.List;
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
                break;
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
            ByteArray buf=null;
            try {
                send(req);
            }
            catch(Exception e) {
                log.warn("failed serializing/sending request", e);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        int                    port=12500;
        int                    recv_buf_size=0, max_length=0;
        long                   expiry_time=0, reaper_interval=0;
        boolean                diag_enabled=true, diag_enable_udp=true, diag_enable_tcp=false;
        InetAddress            diag_mcast_addr=null, diag_bind_addr=null;
        int                    diag_port=7500, diag_port_range=50, diag_ttl=8, soLinger=-1;
        List<NetworkInterface> diag_bind_interfaces=null;
        String                 diag_passcode=null;
        String                 props="udp.xml", name=null;

        // Use bounded queues for sending (https://issues.redhat.com/browse/JGRP-2759)") in TCP (use_nio=false)
        boolean                non_blocking_sends=false;

        // When sending and non_blocking, how many messages to queue max
        int                    max_send_queue=128;

        TLS tls=new TLS();
        long start=System.currentTimeMillis();
        InetAddress bind_addr=null;
        boolean jmx=false, nio=false;

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            if("-port".equals(args[i])) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if("-bindaddress".equals(args[i]) || "-bind_addr".equals(args[i])) {
                bind_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if("-recv_buf_size".equals(args[i])) {
                recv_buf_size=Integer.parseInt(args[++i]);
                continue;
            }
            if("-expiry".equals(args[i])) {
                expiry_time=Long.parseLong(args[++i]);
                continue;
            }
            if("-reaper_interval".equals(args[i])) {
                reaper_interval=Long.parseLong(args[++i]);
                continue;
            }
            if("-jmx".equals(args[i])) {
                jmx=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if("-solinger".equals(args[i])) {
                soLinger=Integer.parseInt(args[++i]);
                continue;
            }
            if("-nio".equals(args[i])) {
                nio=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if("-non_blocking_sends".equals(args[i])) {
                non_blocking_sends=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if("max_send_queue".equals(args[i])) {
                max_send_queue=Integer.parseInt(args[++i]);
                continue;
            }
            if("-max_length".equals(args[i])) {
                max_length=Integer.parseInt(args[++i]);
                continue;
            }
            if("-diag_enabled".equals(args[i])) {
                diag_enabled=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if("-diag_enable_udp".equals(args[i])) {
                diag_enable_udp=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if("-diag_enable_tcp".equals(args[i])) {
                diag_enable_tcp=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if("-diag_mcast_addr".equals(args[i])) {
                diag_mcast_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if("-diag_bind_addr".equals(args[i])) {
                diag_bind_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if("-diag_port".equals(args[i])) {
                diag_port=Integer.parseInt(args[++i]);
                continue;
            }
            if("-diag_port_range".equals(args[i])) {
                diag_port_range=Integer.parseInt(args[++i]);
                continue;
            }
            if("-diag_ttl".equals(args[i])) {
                diag_ttl=Integer.parseInt(args[++i]);
                continue;
            }
            if("-diag_bind_interfaces".equals(args[i])) {
                diag_bind_interfaces=Util.parseInterfaceList(args[++i]);
                continue;
            }
            if("-diag_passcode".equals(args[i])) {
                diag_passcode=args[++i];
                continue;
            }
            if("-tls_protocol".equals(args[i])) {
                tls.enabled(true).setProtocols(args[++i].split(","));
                continue;
            }
            if("-tls_cipher_suites".equals(args[i])) {
                tls.enabled(true).setCipherSuites(args[++i].split(","));
                continue;
            }
            if("-tls_provider".equals(args[i])) {
                tls.enabled(true).setProvider(args[++i]);
                continue;
            }
            if("-tls_keystore_path".equals(args[i])) {
                tls.enabled(true).setKeystorePath(args[++i]);
                continue;
            }
            if("-tls_keystore_password".equals(args[i])) {
                tls.enabled(true).setKeystorePassword(args[++i]);
                continue;
            }
            if("-tls_keystore_type".equals(args[i])) {
                tls.enabled(true).setKeystoreType(args[++i]);
                continue;
            }
            if("-tls_keystore_alias".equals(args[i])) {
                tls.enabled(true).setKeystoreAlias(args[++i]);
                continue;
            }
            if("-tls_truststore_path".equals(args[i])) {
                tls.enabled(true).setTruststorePath(args[++i]);
                continue;
            }
            if("-tls_truststore_password".equals(args[i])) {
                tls.enabled(true).setTruststorePassword(args[++i]);
                continue;
            }
            if("-tls_truststore_type".equals(args[i])) {
                tls.enabled(true).setTruststoreType(args[++i]);
                continue;
            }
            if("-tls_client_auth".equals(args[i])) {
                tls.enabled(true).setClientAuth(TLSClientAuth.valueOf(args[++i].toUpperCase()));
                continue;
            }
            if("-tls_sni_matcher".equals(args[i])) {
                tls.enabled(true).getSniMatchers().add(SNIHostName.createSNIMatcher(args[++i]));
                continue;
            }
            help();
            return;
        }
        if(tls.enabled() && nio)
            throw new IllegalArgumentException("Cannot use NIO with TLS");

        JChannelServer server=new JChannelServer(bind_addr, port, props)
          .name(name)
          .jmx(jmx).expiryTime(expiry_time).reaperInterval(reaper_interval)
          .useNio(nio).recvBufferSize(recv_buf_size).lingerTimeout(soLinger).maxLength(max_length)
          .tls(tls).nonBlockingSends(non_blocking_sends).maxSendQueue(max_send_queue);
        server.diagHandler().setEnabled(diag_enabled)
          .enableUdp(diag_enable_udp)
          .enableTcp(diag_enable_tcp)
          .setMcastAddress(diag_mcast_addr)
          .setBindAddress(diag_bind_addr)
          .setPort(diag_port)
          .setPortRange(diag_port_range)
          .setTtl(diag_ttl)
          .setBindInterfaces(diag_bind_interfaces)
          .setPasscode(diag_passcode);
        String type="";
        if(tls.enabled()) {
            tls.init();
            SSLContext context=tls.createContext();
            SocketFactory socket_factory=tls.createSocketFactory(context);
            server.socketFactory(socket_factory);
            type=String.format(" (%s/%s)", tls.getProtocols()!=null?String.join(",",tls.getProtocols()):"TLS",
                               context.getProvider());
        }
        server.start();
        long time=System.currentTimeMillis()-start;
        IpAddress local=(IpAddress)server.localAddress();
        System.out.printf("\nJChannelServer started in %d ms listening on %s:%s%s\n",
                          time, bind_addr != null? bind_addr : "0.0.0.0",  local.getPort(), type);
    }



    static void help() {
        System.out.println();
        System.out.println("JChannelServer [-port <port>] [-bind_addr <address>] [-props <config>] [-name <name>]" +
                             " [options]\n");
        System.out.println("Options:");
        System.out.println();
        System.out.println("    -jmx <true|false>       - Expose attributes and operations via JMX.\n");
        System.out.println("    -recv_buf_size <bytes>  - Sets the receive buffer");
        System.out.println();
        System.out.println("    -solinger <msecs>       - Time for setting SO_LINGER on connections");
        System.out.println();
        System.out.println("    -expiry <msecs>         - Time for closing idle connections. 0 means don't expire.");
        System.out.println();
        System.out.println("    -reaper_interval <ms>   - Time for check for expired connections. 0 means don't check.");
        System.out.println();
        System.out.println("    -nio <true|false>       - Whether or not to use non-blocking connections (NIO)");
        System.out.println();
        System.out.println("    -non_blocking_sends <true|false> - Use bounded queues for sending (https://issues.redhat.com/browse/JGRP-2759))");
        System.out.println();
        System.out.println("    -max_send_queue <size>  - When sending and non_blocking, how many messages to queue max");
        System.out.println();
        System.out.println("    -max_length <bytes>     - The max size (in bytes) of a message");
        System.out.println();
        System.out.println("    -diag_enabled <true|false> -Enable diagnostics");
        System.out.println();
        System.out.println("    -diag_enable_udp        - Use a multicast socket to listen for probe requests");
        System.out.println();
        System.out.println("    -diag_enable_tcp        - Use a TCP socket to listen for probe requests");
        System.out.println();
        System.out.println("    -diag_mcast_addr        - Multicast address for diagnostic probing (UDP MulticastSocket).");
        System.out.println("                              Used when enable_udp==true");
        System.out.println();
        System.out.println("    -diag_bind_addr         - The bind address of the TCP socket");
        System.out.println();
        System.out.println("    -diag_port              - Port for diagnostics probing. Default is 7500");
        System.out.println();
        System.out.println("    -diag_port_range        - The number of ports to be probed for an available port (TCP)");
        System.out.println();
        System.out.println("    -diag_ttl               - TTL of the diagnostics multicast socket");
        System.out.println();
        System.out.println("    -diag_bind_interfaces   - Comma delimited list of interfaces (IP addrs or interface names)");
        System.out.println("                              that the multicast socket should bind to");
        System.out.println();
        System.out.println("    -diag_passcode          - Authorization passcode for diagnostics. If specified, every probe query will be authorized");
        System.out.println();
        System.out.println("    -tls_protocol <protos>  - One or more TLS protocol names to use, e.g. TLSv1.2.");
        System.out.println("                              Setting this requires configuring key and trust stores.");
        System.out.println();
        System.out.println("    -tls_provider <name>    - The name of the security provider to use for TLS.");
        System.out.println();
        System.out.println("    -tls_keystore_path <file> - The keystore path which contains the key.");
        System.out.println();
        System.out.println("    -tls_keystore_password <password> - The key store password.");
        System.out.println();
        System.out.println("    -tls_keystore_type <type>    - The type of keystore.");
        System.out.println();
        System.out.println("    -tls_keystore_alias <alias>  - The alias of the key to use as identity for this Gossip router.");
        System.out.println();
        System.out.println("    -tls_truststore_path <file>  - The truststore path.");
        System.out.println();
        System.out.println("    -tls_truststore_password <password> - The trust store password.");
        System.out.println();
        System.out.println("    -tls_truststore_type <type>  - The truststore type.");
        System.out.println();
        System.out.println("    -tls_sni_matcher <name>      - A regular expression that servers use to match and accept SNI host names.");
        System.out.println("                                   Can be repeated multiple times.");
        System.out.println();
        System.out.println("    -tls_client_auth <mode>      - none (default), want or need. Whether to require client");
        System.out.println("                                   certificate authentication.");
        System.out.println();
    }
}
