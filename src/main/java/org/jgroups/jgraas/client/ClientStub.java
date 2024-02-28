package org.jgroups.jgraas.client;

import org.jgroups.Address;
import org.jgroups.annotations.GuardedBy;
import org.jgroups.blocks.cs.*;
import org.jgroups.jgraas.server.JChannelServer;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.PingData;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ByteArray;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * Client stub that talks to a remote Server via blocking or non-blocking TCP
 * @author Bela Ban
 */
public class ClientStub extends ReceiverAdapter implements Comparable<ClientStub>, ConnectionListener {
    public interface MembersNotification {void members(List<PingData> mbrs);}
    public interface CloseListener       {void closed(ClientStub stub);}

    protected BaseServer        client;
    protected IpAddress         local;     // bind address
    protected IpAddress         remote;    // address of remote Server
    protected InetSocketAddress remote_sa; // address of remote Server, not resolved yet
    protected final boolean     use_nio;
    protected Receiver          receiver;  // external consumer of data, e.g. TUNNEL
    protected CloseListener     close_listener;
    protected SocketFactory     socket_factory;
    protected static final Log  log=LogFactory.getLog(ClientStub.class);

    // max number of ms to wait for socket establishment to Server
    protected int               sock_conn_timeout=3000;
    protected boolean           tcp_nodelay=true;
    protected int               linger=-1; // SO_LINGER (number of seconds, -1 disables it)
    protected boolean           handle_heartbeats;
    // timestamp of last heartbeat (or message from Server)
    protected volatile long     last_heartbeat;

    // Use bounded queues for sending (https://issues.redhat.com/browse/JGRP-2759)") in TCP
    protected boolean           non_blocking_sends;

    // When sending and non_blocking, how many messages to queue max
    protected int               max_send_queue=128;

    // map to correlate GET_MBRS requests and responses
    protected final Map<String,List<MembersNotification>> get_members_map=new HashMap<>();


    public ClientStub(InetSocketAddress local_sa, InetSocketAddress remote_sa, boolean use_nio, CloseListener l, SocketFactory sf) {
       this(local_sa, remote_sa, use_nio, l, sf, -1);
    }

    public ClientStub(InetSocketAddress local_sa, InetSocketAddress remote_sa, boolean use_nio, CloseListener l,
                      SocketFactory sf, int linger) {
        this(local_sa, remote_sa, use_nio, l, sf, linger, false, 0);
    }

    /**
     * Creates a stub to a remote_sa {@link JChannelServer}.
     * @param local_sa The local_sa bind address and port
     * @param remote_sa The address:port of the server
     * @param use_nio Whether to use ({@link org.jgroups.protocols.TCP_NIO2}) or {@link org.jgroups.protocols.TCP}
     * @param l The {@link CloseListener}
     * @param sf The {@link SocketFactory} to use to create the client socket
     * @param linger SO_LINGER timeout
     * @param non_blocking_sends When true and a TcpClient is used, non-blocking sends are enabled
     *                           (https://issues.redhat.com/browse/JGRP-2759)
     * @param max_send_queue The max size of the send queue for non-blocking sends
     */
    public ClientStub(InetSocketAddress local_sa, InetSocketAddress remote_sa, boolean use_nio, CloseListener l,
                      SocketFactory sf, int linger, boolean non_blocking_sends, int max_send_queue) {
        this.local=local_sa != null? new IpAddress(local_sa.getAddress(), local_sa.getPort())
          : new IpAddress((InetAddress)null,0);
        this.remote_sa=Objects.requireNonNull(remote_sa);
        this.use_nio=use_nio;
        this.close_listener=l;
        this.socket_factory=sf;
        this.linger=linger;
        this.non_blocking_sends=non_blocking_sends;
        this.max_send_queue=max_send_queue;
        if(resolveRemoteAddress()) // sets remote
            client=createClient(sf);
    }


    public IpAddress     local()                              {return local;}
    public IpAddress     remote()                             {return remote;}
    public ClientStub    receiver(Receiver r)                 {receiver=r; return this;}
    public Receiver      receiver()                           {return receiver;}
    public boolean       tcpNoDelay()                         {return tcp_nodelay;}
    public ClientStub    tcpNoDelay(boolean tcp_nodelay)      {this.tcp_nodelay=tcp_nodelay; return this;}
    public CloseListener connectionListener()                 {return close_listener;}
    public ClientStub    connectionListener(CloseListener l)  {this.close_listener=l; return this;}
    public int           socketConnectionTimeout()            {return sock_conn_timeout;}
    public ClientStub    socketConnectionTimeout(int timeout) {this.sock_conn_timeout=timeout; return this;}
    public boolean       useNio()                             {return use_nio;}
    public IpAddress     gossipRouterAddress()                {return remote;}
    public boolean       isConnected()                        {return client != null && ((TcpClient)client).isConnected();}
    public ClientStub    handleHeartbeats(boolean f)          {handle_heartbeats=f; return this;}
    public boolean       handleHeartbeats()                   {return handle_heartbeats;}
    public long          lastHeartbeat()                      {return last_heartbeat;}
    public int           getLinger()                          {return linger;}
    public ClientStub    setLinger(int l)                     {this.linger=l; return this;}
    public boolean       nonBlockingSends()                   {return non_blocking_sends;}
    public ClientStub    nonBlockingSends(boolean b)          {this.non_blocking_sends=b; return this;}
    public int           maxSendQueue()                       {return max_send_queue;}
    public ClientStub    maxSendQueue(int s)                  {this.max_send_queue=s; return this;}


    /** Creates a connection to the remote server */
    public synchronized void connect() throws Exception {
        synchronized(this) {
            _doConnect();
        }
        if(handle_heartbeats)
            last_heartbeat=System.currentTimeMillis();
    }

    @GuardedBy("lock")
    protected void _doConnect() throws Exception {
        if(client != null)
            client.start();
        else {
            if(resolveRemoteAddress() && (client=createClient(this.socket_factory)) != null)
                client.start();
            else
                throw new IllegalStateException("client could not be created as remote address has not yet been resolved");
        }
    }

    public void destroy() {
        Util.close(client);
    }


    @Override
    public void receive(Address sender, byte[] buf, int offset, int length) {
        receive(sender, new ByteArrayDataInputStream(buf, offset, length), length);
    }

    @Override
    public void receive(Address sender, DataInput in, int length) {
        try {
            if(receiver != null)
                receiver.receive(sender, in, length);
            if(handle_heartbeats)
                last_heartbeat=System.currentTimeMillis();
        } catch(Exception ex) {
            log.error(Util.getMessage("FailedReadingData"), ex);
        }
    }

    @Override
    public void connectionClosed(Connection conn) {
        if(close_listener != null)
            close_listener.closed(this);
    }

    @Override
    public void connectionEstablished(Connection conn) {

    }

    @Override public int compareTo(ClientStub o) {
        return remote.compareTo(o.remote);
    }

    public int hashCode() {return remote.hashCode();}

    public boolean equals(Object obj) {
        return compareTo((ClientStub)obj) == 0;
    }

    public String toString() {
        return String.format("RouterStub[local=%s, router_host=%s %s] - age: %s",
                             client != null? client.localAddress() : "n/a", remote,
                             isConnected()? "connected" : "disconnected",
                             Util.printTime(System.currentTimeMillis()-last_heartbeat, TimeUnit.MILLISECONDS));
    }

    /** Creates remote from remote_sa. If the latter is unresolved, tries to resolve it one more time (e.g. via DNS) */
    protected boolean resolveRemoteAddress() {
        if(this.remote != null)
            return true;
        if(this.remote_sa.isUnresolved()) {
            this.remote_sa=new InetSocketAddress(remote_sa.getHostString(), remote_sa.getPort());
            if(this.remote_sa.isUnresolved())
                return false;
        }
        this.remote=new IpAddress(remote_sa.getAddress(), remote_sa.getPort());
        return true;
    }

    protected BaseServer createClient(SocketFactory sf) {
        BaseServer cl=use_nio? new NioClient(local, remote)
          : new TcpClient(local, remote).nonBlockingSends(non_blocking_sends).maxSendQueue(max_send_queue);
        if(sf != null) cl.socketFactory(sf);
        cl.receiver(this);
        cl.addConnectionListener(this);
        cl.socketConnectionTimeout(sock_conn_timeout).tcpNodelay(tcp_nodelay).linger(linger);
        return cl;
    }


    public void send(ByteArray buf) throws Exception {
        client.send(remote, buf.getArray(), buf.getOffset(), buf.getLength());
    }

    public void send(byte[] buf, int offset, int length) throws Exception {
        client.send(remote, buf, offset, length);
    }

    protected void removeResponse(String group, MembersNotification notif) {
        synchronized(get_members_map) {
            List<MembersNotification> set=get_members_map.get(group);
            if(set == null || set.isEmpty()) {
                get_members_map.remove(group);
                return;
            }
            if(set.remove(notif) && set.isEmpty())
                get_members_map.remove(group);
        }
    }

    protected void notifyResponse(String group, List<PingData> list) {
        if(group == null)
            return;
        if(list == null)
            list=Collections.emptyList();
        synchronized(get_members_map) {
            List<MembersNotification> set=get_members_map.get(group);
            while(set != null && !set.isEmpty()) {
                try {
                    MembersNotification rsp=set.remove(0);
                    rsp.members(list);
                }
                catch(Throwable t) {
                    log.error("failed notifying %s: %s", group, t);
                }
            }
            get_members_map.remove(group);
        }
    }


}
