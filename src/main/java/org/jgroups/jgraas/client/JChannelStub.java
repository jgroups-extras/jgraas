package org.jgroups.jgraas.client;

/**
 * Implementation of {@link org.jgroups.JChannel} forwarding requests to a remote
 * {@link org.jgroups.jgraas.server.Server}. {@link ClientStubManager} is used to forward requests to the server, and
 * translate receive requests from the server.
 * process
 * @author Bela Ban
 * @since  5.3.3
 */
public class JChannelStub {
    protected ClientStubManager stub_mgr;

    //stubManager=RouterStubManager.emptyGossipClientStubManager(log, timer).useNio(this.use_nio)
      //        .nonBlockingSends(non_blocking_sends).maxSendQueue(max_send_queue);
}
