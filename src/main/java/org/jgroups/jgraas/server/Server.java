package org.jgroups.jgraas.server;

import org.jgroups.stack.IpAddress;
import org.jgroups.util.SocketFactory;
import org.jgroups.util.TLS;
import org.jgroups.util.TLSClientAuth;
import org.jgroups.util.Util;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.List;

/**
 * Runs the {@link JChannelServer} instance
 * @author Bela Ban
 * @since  1.0.0
 */
public class Server {
    public static void main(String[] args) throws Exception {
        int                    port=12500;
        int                    recv_buf_size=0, max_length=0;
        long                   expiry_time=0, reaper_interval=0;
        boolean                diag_enabled=true, diag_enable_udp=true, diag_enable_tcp=false;
        InetAddress diag_mcast_addr=null, diag_bind_addr=null;
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
