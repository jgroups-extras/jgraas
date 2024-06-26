
package org.jgroups.jgraas.client;

import org.jgroups.blocks.cs.Connection;
import org.jgroups.blocks.cs.ConnectionListener;
import org.jgroups.jgraas.common.ProtoHeartbeat;
import org.jgroups.jgraas.common.ProtoRequest;
import org.jgroups.jgraas.server.JChannelServer;
import org.jgroups.jgraas.util.Utils;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.ByteArray;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Client talking to the remote {@link JChannelServer}
 * @author Bela Ban
 * @since 5.3.3
 */
public class ClientStubManager implements Runnable, ConnectionListener {
    protected final List<ClientStub> stubs=new CopyOnWriteArrayList<>();
    protected final TimeScheduler    timer;
    protected final long             reconnect_interval; // reconnect interval (ms)
    protected boolean                use_nio=true;       // whether to use TcpClient or NioClient
    protected Future<?>              reconnector, heartbeat, timeout_checker;
    protected final Log              log;
    protected SocketFactory          socket_factory;
    // Sends a heartbeat to the server every heartbeat_interval ms (0 disables this)
    protected long                   heartbeat_interval;
    // Max time (ms) with no received message or heartbeat after which the connection to a Server is closed.
    // Ignored when heartbeat_interval is 0.
    protected long                   heartbeat_timeout;
    protected final Runnable         send_heartbeat=this::sendHeartbeat;
    protected final Runnable         check_timeouts=this::checkTimeouts;

    // Use bounded queues for sending (https://issues.redhat.com/browse/JGRP-2759)") in TCP
    protected boolean                non_blocking_sends;

    // When sending and non_blocking, how many messages to queue max
    protected int                    max_send_queue=128;

    protected static final ByteArray HEARTBEAT;

    static {
        try {
            HEARTBEAT=Utils.serialize(ProtoRequest.newBuilder().setHeartbeat(ProtoHeartbeat.newBuilder().build()).build());
        }
        catch(IOException e) {
            throw new RuntimeException(e);
        }
    }


    public ClientStubManager(Log log, TimeScheduler timer, long reconnect_interval) {
        this.log=log != null? log : LogFactory.getLog(ClientStubManager.class);
        this.timer=timer;
        this.reconnect_interval=reconnect_interval;
    }


    public ClientStubManager useNio(boolean flag)            {use_nio=flag; return this;}
    public boolean           reconnectorRunning()            {return reconnector != null && !reconnector.isDone();}
    public boolean           heartbeaterRunning()            {return heartbeat != null && !heartbeat.isDone();}
    public boolean           timeouterRunning()              {return timeout_checker != null && !timeout_checker.isDone();}
    public boolean           nonBlockingSends()              {return non_blocking_sends;}
    public ClientStubManager nonBlockingSends(boolean b)     {this.non_blocking_sends=b; return this;}
    public int               maxSendQueue()                  {return max_send_queue;}
    public ClientStubManager maxSendQueue(int s)             {this.max_send_queue=s; return this;}
    public ClientStubManager socketFactory(SocketFactory sf) {this.socket_factory=sf; return this;}

    public ClientStubManager heartbeat(long heartbeat_interval, long heartbeat_timeout) {
        if(heartbeat_interval <= 0) {
            // disable heartbeating
            stopHeartbeatTask();
            stopTimeoutChecker();
            stubs.forEach(s -> s.handleHeartbeats(false));
            this.heartbeat_interval=0;
            return this;
        }
        if(heartbeat_interval >= heartbeat_timeout)
            throw new IllegalArgumentException(String.format("heartbeat_interval (%d) must be < than heartbeat_timeout (%d)",
                                                             heartbeat_interval, heartbeat_timeout));
        // enable heartbeating
        this.heartbeat_interval=heartbeat_interval;
        this.heartbeat_timeout=heartbeat_timeout;
        stubs.forEach(s -> s.handleHeartbeats(true));
        startHeartbeatTask();
        startTimeoutChecker();
        return this;
    }


    /**
     * Applies action to all connected RouterStubs
     */
    public void forEach(Consumer<ClientStub> action) {
        stubs.stream().filter(ClientStub::isConnected).forEach(action);
    }

    /**
     * Applies action to a randomly picked RouterStub that's connected
     * @param action
     */
    public void forAny(Consumer<ClientStub> action) {
        ClientStub stub=findRandomConnectedStub();
        if(stub != null)
            action.accept(stub);
    }


    public ClientStub createAndRegisterStub(InetSocketAddress local, InetSocketAddress router_addr) {
        return createAndRegisterStub(local, router_addr, -1);
    }

    public ClientStub createAndRegisterStub(InetSocketAddress local, InetSocketAddress router_addr, int linger) {
        ClientStub stub=new ClientStub(local, router_addr, use_nio, this, socket_factory, linger, non_blocking_sends, max_send_queue)
          .handleHeartbeats(heartbeat_interval > 0);
        this.stubs.add(stub);
        return stub;
    }

    public ClientStub unregisterStub(InetSocketAddress router_addr_sa) {
        ClientStub s=stubs.stream().filter(st -> Objects.equals(st.remote_sa, router_addr_sa)).findFirst().orElse(null);
        if(s != null) {
            s.destroy();
            stubs.remove(s);
        }
        return s;
    }


    public void connectStubs() {
        boolean failed_connect_attempts=false;
        for(ClientStub stub: stubs) {
            if(!stub.isConnected()) {
                try {
                    stub.connect();
                }
                catch(Exception ex) {
                    failed_connect_attempts=true;
                }
            }
        }
        if(failed_connect_attempts)
            startReconnector();
    }


    public void destroyStubs() {
        stopReconnector();
        stubs.forEach(ClientStub::destroy);
        stubs.clear();
    }

    public String printStubs() {
        return Util.printListWithDelimiter(stubs, ", ");
    }

    public String printReconnectList() {
        return stubs.stream().filter(s -> !s.isConnected())
          .map(s -> String.format("%s:%d", s.remote_sa.getHostString(), s.remote_sa.getPort()))
          .collect(Collectors.joining(", "));
    }

    public String print() {
        return String.format("Stubs: %s\nReconnect list: %s", printStubs(), printReconnectList());
    }

    public void run() {
        int failed_reconnect_attempts=0;
        for(ClientStub stub: stubs) {
            if(!stub.isConnected()) {
                try {
                    stub.connect();
                    log.debug("%s: re-established connection to server %s", stub.local(), stub.remote());
                }
                catch(Exception ex) {
                    failed_reconnect_attempts++;
                }
            }
        }
        if(failed_reconnect_attempts == 0)
            stopReconnector();
    }



    @Override
    public void connectionEstablished(Connection conn) {
        // System.out.printf("-- connection established: %s\n", conn);
    }

    @Override
    public void connectionClosed(Connection conn) {

        // System.out.printf("closed connection %s; starting reconnector task\n", conn);

        if(log.isDebugEnabled())
            log.debug("closed connection %s; starting reconnector task", conn);
        startReconnector();
    }


    protected synchronized void startReconnector() {
        if(reconnector == null || reconnector.isDone())
            reconnector=timer.scheduleWithFixedDelay(this, reconnect_interval, reconnect_interval, MILLISECONDS);
    }

    protected synchronized void stopReconnector() {
        if(reconnector != null)
            reconnector.cancel(true);
    }

    protected synchronized void startHeartbeatTask() {
        if(heartbeat == null || heartbeat.isDone())
            heartbeat=timer.scheduleWithFixedDelay(send_heartbeat, heartbeat_interval, heartbeat_interval, MILLISECONDS);
    }

    protected synchronized void stopHeartbeatTask() {
        stopTimeoutChecker();
        if(heartbeat != null)
            heartbeat.cancel(true);
    }

    protected synchronized void startTimeoutChecker() {
        if(timeout_checker == null || timeout_checker.isDone())
            timeout_checker=timer.scheduleWithFixedDelay(check_timeouts, heartbeat_timeout, heartbeat_timeout, MILLISECONDS);
    }

    protected synchronized void stopTimeoutChecker() {
        if(timeout_checker != null)
            timeout_checker.cancel(true);
    }

    protected ClientStub findRandomConnectedStub() {
        ClientStub stub=null;
        while(connectedStubs() > 0) {
            ClientStub tmp=Util.pickRandomElement(stubs);
            if(tmp != null && tmp.isConnected())
                return tmp;
        }
        return stub;
    }

    // in upgrade scenarios: the first stub, e.g. version 1
    protected ClientStub getPrimary() {
        return stubs.isEmpty()? null : stubs.get(0);
    }

    // in upgrade scenarios: the second stub, e.g. version 2
    protected ClientStub getSecondary() {
        return stubs.size() > 1? stubs.get(1) : null;
    }

    protected void sendHeartbeat() {
        forEach(s -> {
            try {
                s.send(HEARTBEAT);
            }
            catch(Exception ex) {
                log.error("failed sending heartbeat", ex);
            }
        });
    }

    protected void checkTimeouts() {
        forEach(st -> {
            long timeout=System.currentTimeMillis() - st.lastHeartbeat();
            if(timeout > heartbeat_timeout) {
                log.debug("%s: closed connection to server %s as no heartbeat has been received for %s",
                          st.local(), st.remote(), Util.printTime(timeout, MILLISECONDS));
                st.destroy();
            }
        });
        if(disconnectedStubs())
            startReconnector();
    }

    public int connectedStubs() {
        return (int)stubs.stream().filter(ClientStub::isConnected).count();
    }

    public boolean disconnectedStubs() {
        return stubs.stream().anyMatch(st -> !st.isConnected());
    }

}
