package com.basic.rdmachannel.read;

import com.basic.rdmachannel.channel.RdmaChannel;
import com.basic.rdmachannel.channel.RdmaChannelConf;
import com.basic.rdmachannel.channel.RdmaNode;
import com.basic.rdmachannel.mr.RdmaBuffer;
import com.basic.rdmachannel.mr.RdmaBufferManager;
import com.basic.rdmachannel.util.RDMAUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * locate com.ibm.disni.channel
 * Created by MasterTj on 2019/1/22.
 * node24
 * java -cp rdmachannel-example-1.0-SNAPSHOT-jar-with-dependencies.jar com.basic.rdmachannel.read.RdmaReadClient
 */
public class RdmaReadClient {
    private static final Logger logger = LoggerFactory.getLogger(RdmaReadClient.class);

    public static void main(String[] args) throws Exception {
        String hostName = RDMAUtils.getLocalHostLANAddress("ib0").getHostName();
        RdmaNode rdmaClient=new RdmaNode(hostName,1955, new RdmaChannelConf(), RdmaChannel.RdmaChannelType.RDMA_READ_RESPONDER);

        RdmaChannel rdmaChannel = rdmaClient.getRdmaChannel(new InetSocketAddress("10.10.0.25", 1955), true, RdmaChannel.RdmaChannelType.RDMA_READ_RESPONDER);

        RdmaBufferManager rdmaBufferManager = rdmaClient.getRdmaBufferManager();
        RdmaBuffer rdmaData = rdmaBufferManager.get(4096);
        ByteBuffer dataBuffer = rdmaData.getByteBuffer();
        String str="Hello! I am Client!";
        dataBuffer.asCharBuffer().put(str);
        dataBuffer.flip();

        rdmaClient.sendRegionTokenToRemote(rdmaChannel,rdmaData.createRegionToken());

        Thread.sleep(Integer.MAX_VALUE);
    }
}
