package org.jgroups.jgraas.client;

import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

import java.util.List;

/**
 * Implementation of a protocol stack: most methods are mocked, and - where necessary - methods are forwarded to the
 * server
 * @author Bela Ban
 * @since  5.3.5
 */
public class ClientProtocolStack extends ProtocolStack {
    @Override
    public <T extends Protocol> T findProtocol(String name) {
        return null;
    }

    @Override
    public <T extends Protocol> List<T> findProtocols(String regexp) {
        return null;
    }

    @Override
    public <T extends Protocol> T findProtocol(Class<? extends Protocol> clazz) {
        return null;
    }
}
