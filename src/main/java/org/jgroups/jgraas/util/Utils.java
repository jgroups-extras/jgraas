package org.jgroups.jgraas.util;

import com.google.protobuf.ByteString;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.*;
import org.jgroups.blocks.RequestCorrelator;
import org.jgroups.jgraas.common.*;
import org.jgroups.protocols.FORK;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.util.NameCache;
import org.jgroups.util.Util;

import java.lang.Exception;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.jgroups.jgraas.common.Message.newBuilder;


/**
 * Common methods to convert between JGroups and protobuf requests
 * @author Bela Ban
 * @since  5.3.3
 */
public class Utils {


    protected static org.jgroups.jgraas.common.Message jgMessageToProtoMessage(String cluster, Message m,
                                                                               Marshaller marshaller)
      throws Exception {
        Metadata md=Metadata.newBuilder().setMsgType(m.getType()).build();
        // Sets cluster name, destination and sender addresses, flags and metadata (e.g. message type)
        org.jgroups.jgraas.common.Message.Builder builder=msgBuilder(cluster, m.getSrc(), m.getDest(), m.getFlags(), md);

        // Sets the headers
        List<org.jgroups.jgraas.common.Header> proto_hdrs=jgHeadersToProtoHeaders(((BaseMessage)m).headers());
        builder.addAllHeaders(proto_hdrs);
        boolean is_rsp=proto_hdrs != null && proto_hdrs.stream()
          .anyMatch(h -> h.hasRpcHdr() && h.getRpcHdr().getType() > 0); // 1=RSP, 2=EXC_RSP

        // Sets the payload
        org.jgroups.jgraas.common.ByteArray payload;
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


    protected static Message protoMessageToJGMessage(org.jgroups.jgraas.common.Message msg, MessageFactory msg_factory,
                                                     Marshaller marshaller)
      throws Exception {
        ByteString payload=msg.getPayload();
        Message jg_msg=msg.hasMetaData()? msg_factory.create((short)msg.getMetaData().getMsgType())
          : new BytesMessage();
        if(msg.hasDestination())
            jg_msg.setDest(protoAddressToJGAddress(msg.getDestination()));
        if(msg.hasSender())
            jg_msg.setSrc(protoAddressToJGAddress(msg.getSender()));
        jg_msg.setFlag((short)msg.getFlags(), false);
        boolean is_rsp=false;

        // Headers
        List<org.jgroups.jgraas.common.Header> headers=msg.getHeadersList();
        if(headers != null) {
            is_rsp=headers.stream().anyMatch(h -> h.hasRpcHdr() && h.getRpcHdr().getType() > 0); // 1=RSP, 2=EXC_RSP
            org.jgroups.Header[] hdrs=protoHeadersToJGHeaders(headers);
            if(hdrs != null)
                ((BaseMessage)jg_msg).headers(hdrs);
        }

        // Payload
        if(!payload.isEmpty()) {
            byte[] tmp=payload.toByteArray();
            // if((is_rsp || rpcs) && marshaller != null) {
            if(is_rsp && marshaller != null) {
                Object obj=marshaller.objectFromBuffer(tmp, 0, tmp.length);
                jg_msg.setPayload(obj);
            }
            else {
                if(jg_msg.hasArray())
                    jg_msg.setArray(tmp);
                else {
                    Object pl=Util.objectFromByteBuffer(tmp); // this is NOT compatible between different versions!
                    jg_msg.setObject(pl);
                }
            }
        }
        return jg_msg;
    }


    protected static List<org.jgroups.jgraas.common.Header> jgHeadersToProtoHeaders(org.jgroups.Header[] jg_hdrs) {
        if(jg_hdrs == null || jg_hdrs.length == 0)
            return List.of();
        List<org.jgroups.jgraas.common.Header> l=new ArrayList<>(jg_hdrs.length);
        for(org.jgroups.Header h: jg_hdrs) {
            org.jgroups.jgraas.common.Header hdr=null;
            if(h instanceof RequestCorrelator.Header) {
                RpcHeader rpc_hdr=jgRpcHeaderToProto((RequestCorrelator.Header)h);
                hdr=org.jgroups.jgraas.common.Header.newBuilder().setRpcHdr(rpc_hdr).setProtocolId(h.getProtId()).build();
            }
            else if(h instanceof FORK.ForkHeader) {
                ForkHeader fh=jgForkHeaderToProto((org.jgroups.protocols.FORK.ForkHeader)h);
                hdr=org.jgroups.jgraas.common.Header.newBuilder().setForkHdr(fh).setProtocolId(h.getProtId()).build();
            }
            if(hdr != null)
                l.add(hdr);
        }
        return l;
    }

    protected static org.jgroups.Header[] protoHeadersToJGHeaders(List<org.jgroups.jgraas.common.Header> proto_hdrs) {
        if(proto_hdrs == null || proto_hdrs.isEmpty())
            return null;
        org.jgroups.Header[] retval=new org.jgroups.Header[proto_hdrs.size()];
        int index=0;
        for(org.jgroups.jgraas.common.Header h: proto_hdrs) {
            if(h.hasRpcHdr())
                retval[index++]=protoRpcHeaderToJG(h.getRpcHdr()).setProtId((short)h.getProtocolId());
            else if(h.hasForkHdr())
                retval[index++]=protoForkHeaderToJG(h.getForkHdr()).setProtId((short)h.getProtocolId());
        }
        return retval;
    }

    protected static RpcHeader jgRpcHeaderToProto(RequestCorrelator.Header hdr) {
        RpcHeader.Builder builder = RpcHeader.newBuilder().setType(hdr.type).setRequestId(hdr.req_id).setCorrId(hdr.corrId);
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

    protected static RequestCorrelator.Header protoRpcHeaderToJG(RpcHeader hdr) {
        return new RequestCorrelator.Header((byte)hdr.getType(), hdr.getRequestId(), (short)hdr.getCorrId());
    }


    protected static ForkHeader jgForkHeaderToProto(FORK.ForkHeader h) {
        return ForkHeader.newBuilder().setForkStackId(h.getForkStackId()).setForkChannelId(h.getForkChannelId()).build();
    }

    protected static org.jgroups.Header protoForkHeaderToJG(ForkHeader h) {
        return new FORK.ForkHeader(h.getForkStackId(), h.getForkChannelId());
    }

    protected static org.jgroups.jgraas.common.Message.Builder msgBuilder(String cluster, Address src, Address dest,
                                                                          short flags, Metadata md) {
        org.jgroups.jgraas.common.Message.Builder builder=newBuilder().setClusterName(cluster);
        if(dest !=null)
            builder.setDestination(jgAddressToProtoAddress(dest));
        if(src != null)
            builder.setSender(jgAddressToProtoAddress(src));
        if(md != null)
            builder.setMetaData(md);
        return builder.setFlags(flags);
    }



    protected static org.jgroups.jgraas.common.Address jgAddressToProtoAddress(Address jgroups_addr) {
        if(jgroups_addr == null)
            return org.jgroups.jgraas.common.Address.newBuilder().build();
        if(!(jgroups_addr instanceof org.jgroups.util.UUID))
            throw new IllegalArgumentException(String.format("JGroups address has to be of type UUID but is %s",
                                                             jgroups_addr.getClass().getSimpleName()));
        org.jgroups.util.UUID uuid=(org.jgroups.util.UUID)jgroups_addr;
        String name=jgroups_addr instanceof SiteUUID? ((SiteUUID)jgroups_addr).getName() : NameCache.get(jgroups_addr);

        org.jgroups.jgraas.common.Address.Builder addr_builder=org.jgroups.jgraas.common.Address.newBuilder();
        org.jgroups.jgraas.common.UUID pbuf_uuid=org.jgroups.jgraas.common.UUID.newBuilder()
          .setLeastSig(uuid.getLeastSignificantBits()).setMostSig(uuid.getMostSignificantBits()).build();

        if(jgroups_addr instanceof SiteUUID || jgroups_addr instanceof SiteMaster) {
            String site_name=((SiteUUID)jgroups_addr).getSite();
            org.jgroups.jgraas.common.SiteUUID.Builder b=org.jgroups.jgraas.common.SiteUUID.newBuilder().
              setUuid(pbuf_uuid);
            if(site_name != null)
                b.setSiteName(site_name);
            if(jgroups_addr instanceof SiteMaster)
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

    protected static Address protoAddressToJGAddress(org.jgroups.jgraas.common.Address pbuf_addr) {
        if(pbuf_addr == null)
            return null;

        String logical_name=pbuf_addr.getName();
        Address retval=null;
        if(pbuf_addr.hasSiteUuid()) {
            org.jgroups.jgraas.common.SiteUUID pbuf_site_uuid=pbuf_addr.getSiteUuid();
            String site_name=pbuf_site_uuid.getSiteName();
            if(pbuf_site_uuid.getIsSiteMaster())
                retval=new SiteMaster(site_name);
            else {
                long least=pbuf_site_uuid.getUuid().getLeastSig(), most=pbuf_site_uuid.getUuid().getMostSig();
                retval=new SiteUUID(most, least, logical_name, site_name);
            }
        }
        else if(pbuf_addr.hasUuid()) {
            org.jgroups.jgraas.common.UUID pbuf_uuid=pbuf_addr.getUuid();
            retval=new org.jgroups.util.UUID(pbuf_uuid.getMostSig(), pbuf_uuid.getLeastSig());
        }

        if(retval != null && logical_name != null && !logical_name.isEmpty())
            NameCache.add(retval, logical_name);
        return retval;
    }

    protected static org.jgroups.jgraas.common.View jgViewToProtoView(View v) {
        org.jgroups.jgraas.common.ViewId view_id=jgViewIdToProtoViewId(v.getViewId());
        List<org.jgroups.jgraas.common.Address> mbrs=new ArrayList<>(v.size());
        for(Address a: v)
            mbrs.add(jgAddressToProtoAddress(a));
        return org.jgroups.jgraas.common.View.newBuilder().addAllMember(mbrs).setViewId(view_id).build();
    }

    protected static org.jgroups.jgraas.common.ViewId jgViewIdToProtoViewId(org.jgroups.ViewId view_id) {
        org.jgroups.jgraas.common.Address coord=jgAddressToProtoAddress(view_id.getCreator());
        return org.jgroups.jgraas.common.ViewId.newBuilder().setCreator(coord).setId(view_id.getId()).build();
    }

    protected static org.jgroups.View protoViewToJGView(org.jgroups.jgraas.common.View v) {
        org.jgroups.jgraas.common.ViewId vid=v.getViewId();
        List<org.jgroups.jgraas.common.Address> pbuf_mbrs=v.getMemberList();
        org.jgroups.ViewId jg_vid=new org.jgroups.ViewId(protoAddressToJGAddress(vid.getCreator()), vid.getId());
        List<Address> members=pbuf_mbrs.stream().map(Utils::protoAddressToJGAddress).collect(Collectors.toList());
        return new org.jgroups.View(jg_vid, members);
    }


}
