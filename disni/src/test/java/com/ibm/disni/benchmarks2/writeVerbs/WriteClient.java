package com.ibm.disni.benchmarks2.writeVerbs;

import com.ibm.disni.benchmarks.RdmaBenchmarkCmdLine;
import com.ibm.disni.rdma.RdmaEndpoint;
import com.ibm.disni.rdma.RdmaEndpointFactory;
import com.ibm.disni.rdma.RdmaEndpointGroup;
import com.ibm.disni.rdma.RdmaPassiveEndpointGroup;
import com.ibm.disni.rdma.verbs.*;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * RdmaPassiveEndpointGroup 被动模式提供了一个轮询接口,允许应用程序直接从网络队列（完成队列）中获取完成事件。
 * 因此，被动模式通常具有较低的等待时间，但是在大量线程在同一连接上操作的情况下可能遭受争用。
 * 通常，在服务器上使用ActiveEndPoint以及在客户端使用PassiveEndPoint连接是最佳选择。
 * 如果应用程序知道何时将接收消息，则PassiveEndPoint通常是正确的选择，因此可以相应地轮询完成队列。
 * java -cp disni-1.6-jar-with-dependencies.jar:disni-1.6-tests.jar com.ibm.disni.benchmarks2.writeVerbs.WriteClient -a 10.10.0.93 -s 16 -k 1000
 */
public class WriteClient implements RdmaEndpointFactory<WriteClient.WriteClientEndpoint> {
    private RdmaPassiveEndpointGroup<WriteClientEndpoint> endpointGroup;
    private String host;
    private int port;
    private int size;
    private int loop;

    public WriteClient(String host, int port, int size, int loop) throws IOException{
        this.endpointGroup = new RdmaPassiveEndpointGroup<WriteClientEndpoint>(1, 10, 4, 40);
        this.endpointGroup.init(this);
        this.host = host;
        this.port = port;
        this.size = size;
        this.loop = loop;
    }

    @Override
    public WriteClientEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new WriteClientEndpoint(endpointGroup, id, serverSide ,size);
    }

    /**
     * 3.在客户端上，分配Endpoint Group并使用工厂Factory初始化它，创建客户端EndPoint，连接到服务器EndPoint
     * @throws Exception
     */
    public void run() throws Exception {
        //create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.

        //we have passed our own endpoint factory to the group, therefore new endpoints will be of type CustomClientEndpoint
        //let's create a new client endpoint
        WriteClientEndpoint endpoint = endpointGroup.createEndpoint();

        //connect to the server
        InetAddress ipAddress = InetAddress.getByName(host);
        InetSocketAddress address = new InetSocketAddress(ipAddress, port);
        endpoint.connect(address, 1000);
        InetSocketAddress _addr = (InetSocketAddress) endpoint.getDstAddr();
        System.out.println("ReadClient::client connected, address " + _addr.toString());

        //in our custom endpoints we make sure CQ events get stored in a queue, we now query that queue for new CQ events.
        //in this case a new CQ event means we have received some data, i.e., a message from the server
        endpoint.pollUntil();
        ByteBuffer recvBuf = endpoint.getRecvBuf();
        //the message has been received in this buffer
        //it contains some RDMA information sent by the server
        recvBuf.clear();
        long addr = recvBuf.getLong();
        int length = recvBuf.getInt();
        int lkey = recvBuf.getInt();
        recvBuf.clear();
        System.out.println("ReadClient::receiving rdma information, addr " + addr + ", length " + length + ", key " + lkey);
        System.out.println("ReadClient::preparing write operation...");

        //let's prepare a message to be write to the server
        // the dataBuffer has been set as send buffer
        ByteBuffer dataBuf = endpoint.getDataBuf();
        for (int i = 0; i < size / 4; i++) { //填充dataBuffer
            dataBuf.putInt(1024);
        }
        dataBuf.clear();

        //the RDMA information above identifies a RDMA buffer at the server side
        //let's issue a one-sided RDMA write operation to push the content to that buffer
        IbvSendWR sendWR = endpoint.getSendWR();
        sendWR.setWr_id(1001);
        sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_WRITE);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR.getRdma().setRemote_addr(addr);
        sendWR.getRdma().setRkey(lkey);

        long startTime = System.nanoTime();
        //post the operation on the endpoint
        SVCPostSend postSend = endpoint.postSend(endpoint.getWrList_send());
        for (int i = 0; i < loop; i++){
            postSend.getWrMod(0).getSgeMod(0).setLength(size);
            postSend.execute();
            //wait until the operation has completed
            endpoint.pollUntil();

        }
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        double latency = duration / 1e6 / loop;
        System.out.println("ReadClient, duration: " + duration / 1e6 + "ms, loop: " + loop);
        double throughput = loop / (duration / 1e9);
        System.out.println("Readclient, throughput: " + throughput + ", latency: " + latency);

        //let's prepare a final message to signal everything went fine
        sendWR.setWr_id(1002);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        sendWR.getRdma().setRemote_addr(addr);
        sendWR.getRdma().setRkey(lkey);

        //post that operation
        endpoint.postSend(endpoint.getWrList_send()).execute().free();

        //close everything
        System.out.println("closing endpoint");
        endpoint.close();
        System.out.println("closing endpoint, done");
        endpointGroup.close();
    }

    public static void main(String[] args) throws Exception {
        RdmaBenchmarkCmdLine cmdLine = new RdmaBenchmarkCmdLine("ReadClient");
        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }

        WriteClient client = new WriteClient(cmdLine.getIp(), cmdLine.getPort(), cmdLine.getSize(), cmdLine.getLoop());
        client.run();
    }


    /**
     *  1.通过继承RdmaActiveEndpoint来自定义您自己的Endpoint
     */
    public static class WriteClientEndpoint extends RdmaEndpoint {
        private ByteBuffer buffers[];
        private IbvMr mrlist[];
        private int buffercount = 3;
        private int buffersize;

        private ByteBuffer dataBuf;
        private IbvMr dataMr;
        private ByteBuffer sendBuf;
        private IbvMr sendMr;
        private ByteBuffer recvBuf;
        private IbvMr recvMr;

        private LinkedList<IbvSendWR> wrList_send;
        private IbvSge sgeSend;
        private LinkedList<IbvSge> sgeList;
        private IbvSendWR sendWR;

        private LinkedList<IbvRecvWR> wrList_recv;
        private IbvSge sgeRecv;
        private LinkedList<IbvSge> sgeListRecv;
        private IbvRecvWR recvWR;

        private IbvWC[] wcEvents;
        private SVCPollCq poll;

        public WriteClientEndpoint(RdmaEndpointGroup<? extends RdmaEndpoint> endpointGroup, RdmaCmId idPriv, boolean isServerSide, int size) throws IOException {
            super(endpointGroup, idPriv, isServerSide);
            this.buffercount = 3;
            this.buffersize = size;
            buffers = new ByteBuffer[buffercount];
            this.mrlist = new IbvMr[buffercount];

//            for (int i = 0; i < buffercount; i++){
//                buffers[i] = ByteBuffer.allocateDirect(buffersize);
//            }
            //buffers[0]是dataBuffer，read的数据量与它的大小有关
            buffers[0] = ByteBuffer.allocateDirect(buffersize);
            //buffers[1]和buffers[2]的大小设置成16
            buffers[1] = ByteBuffer.allocateDirect(16);
            buffers[2] = ByteBuffer.allocateDirect(16);

            this.wrList_send = new LinkedList<IbvSendWR>();
            this.sgeSend = new IbvSge();
            this.sgeList = new LinkedList<IbvSge>();
            this.sendWR = new IbvSendWR();

            this.wrList_recv = new LinkedList<IbvRecvWR>();
            this.sgeRecv = new IbvSge();
            this.sgeListRecv = new LinkedList<IbvSge>();
            this.recvWR = new IbvRecvWR();

        }

        //important: we override the init method to prepare some buffers (memory registration, post recv, etc).
        //This guarantees that at least one recv operation will be posted at the moment this endpoint is connected.
        public void init() throws IOException{
            super.init();

            IbvCQ cq = getCqProvider().getCQ();
            this.wcEvents = new IbvWC[getCqProvider().getCqSize()];
            for (int i = 0; i < wcEvents.length; i++){
                wcEvents[i] = new IbvWC();
            }
            this.poll = cq.poll(wcEvents, wcEvents.length);

            for (int i = 0; i < buffercount; i++){
                mrlist[i] = registerMemory(buffers[i]).execute().free().getMr();
            }

            this.dataBuf = buffers[0];
            this.dataMr = mrlist[0];
            this.sendBuf = buffers[1];
            this.sendMr= mrlist[1];
            this.recvBuf = buffers[2];
            this.recvMr = mrlist[2];

            dataBuf.clear();
            sendBuf.clear();

            sgeSend.setAddr(dataMr.getAddr());
            sgeSend.setLength(dataMr.getLength());
            sgeSend.setLkey(dataMr.getLkey());
            sgeList.add(sgeSend);
            sendWR.setWr_id(2000);
            sendWR.setSg_list(sgeList);
            sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
            sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
            wrList_send.add(sendWR);

            sgeRecv.setAddr(recvMr.getAddr());
            sgeRecv.setLength(recvMr.getLength());
            int lkey = recvMr.getLkey();
            sgeRecv.setLkey(lkey);
            sgeListRecv.add(sgeRecv);
            recvWR.setSg_list(sgeListRecv);
            recvWR.setWr_id(2001);
            wrList_recv.add(recvWR);

            System.out.println("RdmaPassiveReadClient::initiated recv");
            this.postRecv(wrList_recv).execute().free();
        }

        public LinkedList<IbvSendWR> getWrList_send() {
            return wrList_send;
        }

        public LinkedList<IbvRecvWR> getWrList_recv() {
            return wrList_recv;
        }

        public SVCPostSend newPostSend() throws IOException {
            return this.postSend(wrList_send);
        }

        public ByteBuffer getDataBuf() {
            return dataBuf;
        }

        public ByteBuffer getSendBuf() {
            return sendBuf;
        }

        public ByteBuffer getRecvBuf() {
            return recvBuf;
        }

        public IbvSendWR getSendWR() {
            return sendWR;
        }

        public IbvRecvWR getRecvWR() {
            return recvWR;
        }

        public IbvMr getDataMr() {
            return dataMr;
        }

        /**
         * 轮询等待请求完成
         * @return
         * @throws IOException
         */
        private int pollUntil() throws IOException {
            int res = 0;
            while (res == 0) {
                res = poll.execute().getPolls();
            }
            return res;
        }
    }
}
