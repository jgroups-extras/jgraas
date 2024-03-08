

package org.jgroups.jgraas.demos;


import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.blocks.ReplicatedHashMap;
import org.jgroups.jgraas.client.JChannelStub;
import org.jgroups.util.Util;

import java.net.InetSocketAddress;
import java.util.Map;


/**
 * Uses the ReplicatedHashMap building block, which subclasses java.util.HashMap and overrides
 * the methods that modify the hashmap (e.g. put()). Those methods are multicast to the group, whereas
 * read-only methods such as get() use the local copy. A ReplicatedHashMap is created given the name
 * of a group; all hashmaps with the same name find each other and form a group.
 * @author Bela Ban
 */
public class ReplicatedHashMapDemo implements ReplicatedHashMap.Notification<String,String> {
    ReplicatedHashMap<String,String>  map=null;


    public ReplicatedHashMapDemo() {
        super();
    }


    public void start(JChannel channel) throws Exception {
        map=new ReplicatedHashMap<>(channel);
        map.addNotifier(this);
        map.start(10000); // fetches the state
        System.out.println("help\nput key value\nget key\nlist:");
        for(;;) {
            String line=Util.readLine(System.in).trim();
            if(line.startsWith("help")) {
                h();
                continue;
            }
            if(line.startsWith("put")) {
                line=line.substring(3).trim();
                String[] tmp=line.split(" +");
                String key=tmp[0], value=tmp[1];
                int num_times=tmp.length > 2? Integer.parseInt(tmp[2]) : 0;
                if(num_times > 0) {
                    for(int i=1; i <= num_times; i++)
                        map.put(String.format("%s-%d", key, i), String.format("%s-%d", value, i));
                }
                else
                    map.put(key, value);
                continue;
            }
            if(line.startsWith("list")) {
                System.out.println(map.toString());
                continue;
            }
            if(line.startsWith("get")) {
                line=line.substring(3).trim();
                String[] tmp=line.split(" ");
                String key=tmp[0];
                if(map.containsKey(key))
                    System.out.printf("%s: %s\n", key, map.get(key));
                continue;
            }
            if(line.startsWith("remove")) {
                line=line.substring(3).trim();
                String[] tmp=line.split(" ");
                String key=tmp[0];
                map.remove(key);
                continue;
            }
            if(line.startsWith("clear")) {
                map.clear();
            }
        }
    }

    protected static void h() {
        System.out.println("help\nput <key> <value> [times]\nget <key>\nlist\nclear\nremove <key>:");
    }

    public void entrySet(String key, String value) {
    }

    public void entryRemoved(String key) {
    }

    public void contentsSet(Map<String,String> m) {
        System.out.println("new contents: " + m);
    }

    public void contentsCleared() {
        System.out.println("contents cleared");
    }


    public void viewChange(View view, java.util.List<Address> new_mbrs, java.util.List<Address> old_mbrs) {
        System.out.println("** view: " + view);
    }

    public static void main(String[] args) {
        ReplicatedHashMapDemo client;
        JChannel              channel=null;
        String                props="udp.xml", name=null, server=null;

        try {
            for(int i=0; i < args.length; i++) {
                String arg=args[i];
                if("-props".equals(arg)) {
                    props=args[++i];
                    continue;
                }
                if("-name".equals(args[i])) {
                    name=args[++i];
                    continue;
                }
                if("-server".equals(args[i])) {
                    server=args[++i];
                    continue;
                }
                help();
                return;
            }
        }
        catch(Exception e) {
            help();
            return;
        }
        try {
            if(server != null && !server.isEmpty()) {
                InetSocketAddress srv=Util.parseHost(server);
                channel=new JChannelStub(srv);
                ((JChannelStub)channel).connectToRemote();
            }
            else
                channel=new JChannel(props);
            channel.name(name);
            client=new ReplicatedHashMapDemo();
            channel.connect("rhm");
            client.start(channel);
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
        finally {
            Util.close(channel);
        }
    }


    static void help() {
        System.out.printf("%s [-help] [-props <properties>] [-name <name>] [-server <addr:port>]\n",
                          ReplicatedHashMapDemo.class.getSimpleName());
    }

}
