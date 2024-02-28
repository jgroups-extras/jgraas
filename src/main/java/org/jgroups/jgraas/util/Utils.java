package org.jgroups.jgraas.util;

import com.google.protobuf.ByteString;
import org.jgroups.*;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.jgraas.common.ByteArray;
import org.jgroups.jgraas.common.*;
import org.jgroups.protocols.FORK;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.util.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;



/**
 * Common methods to convert between JGroups and protobuf requests
 * @author Bela Ban
 * @since  5.3.3
 */
public class Utils {
    public static ProtoMessageBatch jgMessageBatchToProto(MessageBatch batch, Marshaller marshaller) {
        AsciiString tmp=batch.clusterName();
        String cluster=tmp != null? tmp.toString() : null;
        ProtoMessageBatch.Builder builder=ProtoMessageBatch.newBuilder()
          .setClusterName(cluster).setDestination(jgAddressToProtoAddress(batch.dest()))
          .setSender(jgAddressToProtoAddress(batch.sender()))
          .setMulticast(batch.multicast()).setMode(jgModeToProto(batch.mode())).setTimestamp(batch.timestamp());
        for(Message msg: batch) {
            try {
                ProtoMessage proto_msg=Utils.jgMessageToProto(cluster, msg, marshaller);
                builder.addMsgs(proto_msg);
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
        return builder.build();
    }

    public static MessageBatch protoMessageBatchToJG(ProtoMessageBatch batch, MessageFactory msg_factory,
                                                     Marshaller marshaller) {
        List<ProtoMessage> msgs=batch.getMsgsList();
        Address dest=Utils.protoAddressToJGAddress(batch.getDestination()), sender=protoAddressToJGAddress(batch.getSender());
        String tmp=batch.getClusterName();
        AsciiString cluster=tmp != null? new AsciiString(tmp) : null;
        MessageBatch.Mode mode=protoModeToJG(batch.getMode());
        MessageBatch mb=new MessageBatch(dest, sender, cluster, batch.getMulticast(), mode, batch.getMsgsCount());
        for(ProtoMessage m: msgs) {
            try {
                Message msg=protoMessageToJG(m, msg_factory, marshaller);
                mb.add(msg);
            }
            catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
        return mb;
    }

    public static ProtoMessage jgMessageToProto(String cluster, Message m, Marshaller marshaller) throws Exception {
        ProtoMetadata md=ProtoMetadata.newBuilder().setMsgType(m.getType()).build();
        // Sets cluster name, destination and sender addresses, flags and metadata (e.g. message type)
        ProtoMessage.Builder builder=msgBuilder(cluster, m.getSrc(), m.getDest(), m.getFlags(), md);

        // Sets the headers
        List<ProtoHeader> proto_hdrs=jgHeadersToProtoHeaders(((BaseMessage)m).headers());
        builder.addAllHeaders(proto_hdrs);
        boolean is_rsp=proto_hdrs != null && proto_hdrs.stream()
          .anyMatch(h -> h.hasRpcHdr() && h.getRpcHdr().getType() > 0); // 1=RSP, 2=EXC_RSP

        // Sets the payload
        ByteArray payload;
        // if((is_rsp || rpcs) && marshaller != null) {
        if(is_rsp && marshaller != null) {
            Object obj=m.getPayload();
            payload=marshaller.objectToBuffer(obj);
        }
        else {
            if(m.hasArray())
                payload=new ByteArray(m.getArray(), m.getOffset(), m.getLength());
            else {
                // todo: objectToByteBuffer()/Message.getObject() are not JGroups version-independent!
                org.jgroups.util.ByteArray pl=m.hasPayload()? Util.objectToBuffer(m.getObject()) : null;
                payload=pl != null? new ByteArray(pl.getArray(), pl.getOffset(), pl.getLength()) : null;
            }
        }
        if(payload != null)
            builder.setPayload(ByteString.copyFrom(payload.getBytes(), payload.getOffset(), payload.getLength()));
        return builder.build();
    }


    public static Message protoMessageToJG(ProtoMessage msg, MessageFactory msg_factory,
                                           Marshaller marshaller) throws Exception {
        ByteString payload=msg.getPayload();
        Message m=msg.hasMetaData()? msg_factory.create((short)msg.getMetaData().getMsgType()) : new BytesMessage();
        if(msg.hasDestination())
            m.setDest(protoAddressToJGAddress(msg.getDestination()));
        if(msg.hasSender())
            m.setSrc(protoAddressToJGAddress(msg.getSender()));
        m.setFlag((short)msg.getFlags(), false);
        boolean is_rsp=false;

        // Headers
        List<ProtoHeader> headers=msg.getHeadersList();
        if(headers != null) {
            is_rsp=headers.stream().anyMatch(h -> h.hasRpcHdr() && h.getRpcHdr().getType() > 0); // 1=RSP, 2=EXC_RSP
            Header[] hdrs=protoHeadersToJGHeaders(headers);
            if(hdrs != null)
                ((BaseMessage)m).headers(hdrs);
        }

        // Payload
        if(!payload.isEmpty()) {
            byte[] tmp=payload.toByteArray();
            // if((is_rsp || rpcs) && marshaller != null) {
            if(is_rsp && marshaller != null) {
                Object obj=marshaller.objectFromBuffer(tmp, 0, tmp.length);
                m.setPayload(obj);
            }
            else {
                if(m.hasArray())
                    m.setArray(tmp);
                else {
                    Object pl=Util.objectFromByteBuffer(tmp); // this is NOT compatible between different versions!
                    m.setObject(pl);
                }
            }
        }
        return m;
    }


    public static List<ProtoHeader> jgHeadersToProtoHeaders(Header[] jg_hdrs) {
        if(jg_hdrs == null || jg_hdrs.length == 0)
            return List.of();
        List<ProtoHeader> l=new ArrayList<>(jg_hdrs.length);
        for(Header h: jg_hdrs) {
            ProtoHeader hdr=null;
            if(h instanceof RequestCorrelator.Header) {
                ProtoRpcHeader rpc_hdr=jgRpcHeaderToProto((RequestCorrelator.Header)h);
                hdr=ProtoHeader.newBuilder().setRpcHdr(rpc_hdr).setProtocolId(h.getProtId()).build();
            }
            else if(h instanceof FORK.ForkHeader) {
                ProtoForkHeader fh=jgForkHeaderToProto((FORK.ForkHeader)h);
                hdr=ProtoHeader.newBuilder().setForkHdr(fh).setProtocolId(h.getProtId()).build();
            }
            if(hdr != null)
                l.add(hdr);
        }
        return l;
    }

    public static Header[] protoHeadersToJGHeaders(List<ProtoHeader> proto_hdrs) {
        if(proto_hdrs == null || proto_hdrs.isEmpty())
            return null;
        Header[] retval=new Header[proto_hdrs.size()];
        int index=0;
        for(ProtoHeader h: proto_hdrs) {
            if(h.hasRpcHdr())
                retval[index++]=protoRpcHeaderToJG(h.getRpcHdr()).setProtId((short)h.getProtocolId());
            else if(h.hasForkHdr())
                retval[index++]=protoForkHeaderToJG(h.getForkHdr()).setProtId((short)h.getProtocolId());
        }
        return retval;
    }

    protected static ProtoRpcHeader jgRpcHeaderToProto(RequestCorrelator.Header hdr) {
        ProtoRpcHeader.Builder builder=ProtoRpcHeader.newBuilder().setType(hdr.type).setRequestId(hdr.req_id).setCorrId(hdr.corrId);
        if (hdr instanceof RequestCorrelator.MultiDestinationHeader) {
            RequestCorrelator.MultiDestinationHeader mdhdr = (RequestCorrelator.MultiDestinationHeader) hdr;
            Address[] exclusions=mdhdr.exclusion_list;
            if(exclusions != null && exclusions.length > 0) {
                builder.addAllExclusionList(Arrays.stream(exclusions)
                                              .map(Utils::jgAddressToProtoAddress)
                                              .collect(Collectors.toList()));
            }
        }
        return builder.build();
    }

    protected static RequestCorrelator.Header protoRpcHeaderToJG(ProtoRpcHeader hdr) {
        return new RequestCorrelator.Header((byte)hdr.getType(), hdr.getRequestId(), (short)hdr.getCorrId());
    }


    protected static ProtoForkHeader jgForkHeaderToProto(FORK.ForkHeader h) {
        return ProtoForkHeader.newBuilder().setForkStackId(h.getForkStackId()).setForkChannelId(h.getForkChannelId()).build();
    }

    protected static Header protoForkHeaderToJG(ProtoForkHeader h) {
        return new FORK.ForkHeader(h.getForkStackId(), h.getForkChannelId());
    }

    protected static ProtoMessage.Builder msgBuilder(String cluster, Address src, Address dest,
                                                     short flags, ProtoMetadata md) {
        ProtoMessage.Builder builder=ProtoMessage.newBuilder().setClusterName(cluster);
        if(dest !=null)
            builder.setDestination(jgAddressToProtoAddress(dest));
        if(src != null)
            builder.setSender(jgAddressToProtoAddress(src));
        if(md != null)
            builder.setMetaData(md);
        return builder.setFlags(flags);
    }

    public static ProtoAddress jgAddressToProtoAddress(Address jg_addr) {
        if(jg_addr == null)
            return ProtoAddress.newBuilder().build();
        if(!(jg_addr instanceof UUID))
            throw new IllegalArgumentException(String.format("JGroups address has to be of type UUID but is %s",
                                                             jg_addr.getClass().getSimpleName()));
        UUID uuid=(UUID)jg_addr;
        String name=jg_addr instanceof SiteUUID? ((SiteUUID)jg_addr).getName() : NameCache.get(jg_addr);

        ProtoAddress.Builder addr_builder=ProtoAddress.newBuilder();
        ProtoUUID pbuf_uuid=ProtoUUID.newBuilder().setLeastSig(uuid.getLeastSignificantBits())
          .setMostSig(uuid.getMostSignificantBits()).build();

        if(jg_addr instanceof SiteUUID || jg_addr instanceof SiteMaster) {
            String site_name=((SiteUUID)jg_addr).getSite();
            ProtoSiteUUID.Builder b=ProtoSiteUUID.newBuilder().setUuid(pbuf_uuid);
            if(site_name != null)
                b.setSiteName(site_name);
            if(jg_addr instanceof SiteMaster)
                b.setIsSiteMaster(true);
            addr_builder.setSiteUuid(b.build());
        }
        else {
            addr_builder.setUuid(pbuf_uuid);
        }
        if(name != null)
            addr_builder.setName(name);
        return addr_builder.build();
    }

    public static Address protoAddressToJGAddress(ProtoAddress pbuf_addr) {
        if(pbuf_addr == null)
            return null;

        String logical_name=pbuf_addr.getName();
        Address retval=null;
        if(pbuf_addr.hasSiteUuid()) {
            ProtoSiteUUID pbuf_site_uuid=pbuf_addr.getSiteUuid();
            String site_name=pbuf_site_uuid.getSiteName();
            if(pbuf_site_uuid.getIsSiteMaster())
                retval=new SiteMaster(site_name);
            else {
                long least=pbuf_site_uuid.getUuid().getLeastSig(), most=pbuf_site_uuid.getUuid().getMostSig();
                retval=new SiteUUID(most, least, logical_name, site_name);
            }
        }
        else if(pbuf_addr.hasUuid()) {
            ProtoUUID pbuf_uuid=pbuf_addr.getUuid();
            retval=new UUID(pbuf_uuid.getMostSig(), pbuf_uuid.getLeastSig());
        }

        if(retval != null && logical_name != null && !logical_name.isEmpty())
            NameCache.add(retval, logical_name);
        return retval;
    }

    public static ProtoView jgViewToProtoView(View v) {
        ProtoViewId view_id=jgViewIdToProtoViewId(v.getViewId());
        List<ProtoAddress> mbrs=new ArrayList<>(v.size());
        for(Address a: v)
            mbrs.add(jgAddressToProtoAddress(a));
        return ProtoView.newBuilder().addAllMember(mbrs).setViewId(view_id).build();
    }

    public static ProtoViewId jgViewIdToProtoViewId(ViewId view_id) {
        ProtoAddress coord=jgAddressToProtoAddress(view_id.getCreator());
        return ProtoViewId.newBuilder().setCreator(coord).setId(view_id.getId()).build();
    }

    public static View protoViewToJGView(ProtoView v) {
        ProtoViewId vid=v.getViewId();
        List<ProtoAddress> pbuf_mbrs=v.getMemberList();
        ViewId jg_vid=new ViewId(protoAddressToJGAddress(vid.getCreator()), vid.getId());
        List<Address> members=pbuf_mbrs.stream().map(Utils::protoAddressToJGAddress).collect(Collectors.toList());
        return new View(jg_vid, members);
    }

    public static ProtoException exceptionToProto(Exception ex) {
        return ProtoException.newBuilder().setClassname(ex.getClass().getName())
          .setMessage(ex.getMessage()).setStacktrace(getStackTrace(ex)).build();
    }

    public static <T extends Throwable> T protoExceptionToJava(ProtoException proto_ex) throws Exception {
        String classname=proto_ex.getClassname(), message=proto_ex.getMessage(), stack=proto_ex.getStacktrace();
        if(stack != null)
            message=String.format("%s: %s", message, stack);

        Class<T> cl=(Class<T>)Util.loadClass(classname, Thread.currentThread().getContextClassLoader());
        return cl.getConstructor(String.class).newInstance(message);
    }

    public static ProtoJoinRequest stringToJoinRequest(String s) {
        return ProtoJoinRequest.newBuilder().setClusterName(s).build();
    }

    public static String joinRequestToString(ProtoJoinRequest req) {
        return req.getClusterName();
    }

    public static org.jgroups.util.ByteArray serialize(ProtoRequest req) throws IOException {
        ExposedByteArrayOutputStream out=new ExposedByteArrayOutputStream(req.getSerializedSize() + Integer.BYTES);
        req.writeDelimitedTo(out); // writes the size first, then the request
        return out.getBuffer();
    }

    public static Mode jgModeToProto(MessageBatch.Mode m) {
        switch(m) {
            case OOB:   return Mode.OOB;
            case REG:   return Mode.REG;
            case MIXED: return Mode.MIXED;
        }
        throw new IllegalArgumentException(String.format("mode %s not known", m));
    }

    public static MessageBatch.Mode protoModeToJG(Mode m) {
        switch(m) {
            case OOB:   return MessageBatch.Mode.OOB;
            case REG:   return MessageBatch.Mode.REG;
            case MIXED: return MessageBatch.Mode.MIXED;
        }
        throw new IllegalArgumentException(String.format("mode %s not known", m));
    }

    public static String getStackTrace(Exception ex) {
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        PrintStream ps=new PrintStream(out, true);
        ex.printStackTrace(ps);
        return out.toString();
    }

}
