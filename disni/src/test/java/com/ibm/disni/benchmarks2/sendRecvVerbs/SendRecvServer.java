/*
 * DiSNI: Direct Storage and Networking Interface
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.disni.benchmarks2.sendRecvVerbs;

import com.ibm.disni.benchmarks.RdmaBenchmarkCmdLine;
import com.ibm.disni.rdma.RdmaActiveEndpoint;
import com.ibm.disni.rdma.RdmaActiveEndpointGroup;
import com.ibm.disni.rdma.RdmaEndpointFactory;
import com.ibm.disni.rdma.RdmaServerEndpoint;
import com.ibm.disni.rdma.verbs.*;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *  DISNI Example SendRecvServer 服务器程序
 *  java -cp disni-1.6-jar-with-dependencies.jar:disni-1.6-tests.jar com.ibm.disni.benchmarks2.sendRecvVerbs.SendRecvServer -a 10.10.0.93 -k 1000 -s 16
 */
public class SendRecvServer implements RdmaEndpointFactory<SendRecvServer.CustomServerEndpoint> {
	RdmaActiveEndpointGroup<SendRecvServer.CustomServerEndpoint> endpointGroup;
	private String host;
	private int port;
	private int size;
	private int loop;

	public SendRecvServer.CustomServerEndpoint createEndpoint(RdmaCmId idPriv, boolean serverSide) throws IOException {
		return new SendRecvServer.CustomServerEndpoint(endpointGroup, idPriv, serverSide, size);
	}

	public void run() throws Exception {
		//create a EndpointGroup. The RdmaActiveEndpointGroup contains CQ processing and delivers CQ event to the endpoint.dispatchCqEvent() method.
		endpointGroup = new RdmaActiveEndpointGroup<SendRecvServer.CustomServerEndpoint>(1000, false, 128, 4, 128);
		endpointGroup.init(this);
		//create a server endpoint
		RdmaServerEndpoint<SendRecvServer.CustomServerEndpoint> serverEndpoint = endpointGroup.createServerEndpoint();

		//we can call bind on a server endpoint, just like we do with sockets
		InetAddress ipAddress = InetAddress.getByName(host);
		InetSocketAddress address = new InetSocketAddress(ipAddress, port);
		serverEndpoint.bind(address, 10);
		System.out.println("SimpleServer::servers bound to address " + address.toString());

		//we can accept new connections
		SendRecvServer.CustomServerEndpoint clientEndpoint = serverEndpoint.accept();
		//we have previously passed our own endpoint factory to the group, therefore new endpoints will be of type CustomServerEndpoint
		System.out.println("SimpleServer::client connection accepted");


        for (int i = 0; i < loop; i++) {
            IbvSge sgeRecv = new IbvSge();
            IbvMr recvMr = clientEndpoint.getRecvMr();
            sgeRecv.setAddr(recvMr.getAddr());
            sgeRecv.setLength(recvMr.getLength());
            sgeRecv.setLkey(recvMr.getLkey());
            LinkedList<IbvSge> sgeListRecv = new LinkedList<>();
            sgeListRecv.add(sgeRecv);

            IbvRecvWR recvWR = new IbvRecvWR();
            recvWR.setSg_list(sgeListRecv);
            recvWR.setWr_id(5000 + i);
            LinkedList<IbvRecvWR> wrList_recv = new LinkedList<>();
            wrList_recv.add(recvWR);

            SVCPostRecv postRecv = clientEndpoint.postRecv(wrList_recv);
            postRecv.execute().free();
            //in our custom endpoints we make sure CQ events get stored in a queue, we now query that queue for new CQ events.
            //in this case a new CQ event means we have received data, i.e., a message from the client.
            LinkedBlockingQueue<IbvWC> wcEvents = clientEndpoint.getWcEvents();
            wcEvents.take();
            System.out.println("SimpleServer, wcEvents length: " + wcEvents.size());
//            System.out.println("SimpleServer::message received");
//            ByteBuffer recvBuf = clientEndpoint.getRecvBuf();
//            recvBuf.clear();
//            for (int j = 0; j < size / 4; j++) {
//                System.out.print(recvBuf.getInt() + ", ");
//            }
//            System.out.println();
        }

		//close everything
		clientEndpoint.close();
		serverEndpoint.close();
		endpointGroup.close();
	}

	public void launch(String[] args) throws Exception {
        RdmaBenchmarkCmdLine cmdLine = new RdmaBenchmarkCmdLine("SendRecvServer");

		try {
			cmdLine.parse(args);
		} catch (ParseException e) {
			cmdLine.printHelp();
			System.exit(-1);
		}
		host = cmdLine.getIp();
		port = cmdLine.getPort();
		size = cmdLine.getSize();
		loop = cmdLine.getLoop();

		this.run();
	}

	public static void main(String[] args) throws Exception {
		SendRecvServer simpleServer = new SendRecvServer();
		simpleServer.launch(args);
	}

	public static class CustomServerEndpoint extends RdmaActiveEndpoint {
		private ByteBuffer buffers[];
		private IbvMr mrlist[];
		private int buffercount = 3;

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

//		private ArrayBlockingQueue<IbvWC> wcEvents;
		private LinkedBlockingQueue<IbvWC> wcEvents;

		public CustomServerEndpoint(RdmaActiveEndpointGroup<CustomServerEndpoint> endpointGroup,
									RdmaCmId idPriv, boolean serverSide, int buffersize) throws IOException {
			super(endpointGroup, idPriv, serverSide);
			this.buffercount = 3;
			buffers = new ByteBuffer[buffercount];
			this.mrlist = new IbvMr[buffercount];

			for (int i = 0; i < buffercount; i++){
				buffers[i] = ByteBuffer.allocateDirect(buffersize);
			}

			this.wrList_send = new LinkedList<IbvSendWR>();
			this.sgeSend = new IbvSge();
			this.sgeList = new LinkedList<IbvSge>();
			this.sendWR = new IbvSendWR();

			this.wrList_recv = new LinkedList<IbvRecvWR>();
			this.sgeRecv = new IbvSge();
			this.sgeListRecv = new LinkedList<IbvSge>();
			this.recvWR = new IbvRecvWR();

//			this.wcEvents = new ArrayBlockingQueue<IbvWC>(10);
            this.wcEvents = new LinkedBlockingQueue<IbvWC>();
		}

		//important: we override the init method to prepare some buffers (memory registration, post recv, etc).
		//This guarantees that at least one recv operation will be posted at the moment this endpoint is connected.
		public void init() throws IOException{
			super.init();

			for (int i = 0; i < buffercount; i++){
				mrlist[i] = registerMemory(buffers[i]).execute().free().getMr();
			}

			this.dataBuf = buffers[0];
			this.dataMr = mrlist[0];
			this.sendBuf = buffers[1];
			this.sendMr = mrlist[1];
			this.recvBuf = buffers[2];
			this.recvMr = mrlist[2];

			sgeSend.setAddr(sendMr.getAddr());
			sgeSend.setLength(sendMr.getLength());
			sgeSend.setLkey(sendMr.getLkey());
			sgeList.add(sgeSend);
			sendWR.setWr_id(2000);
			sendWR.setSg_list(sgeList);
			sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
			sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
			wrList_send.add(sendWR);

//			sgeRecv.setAddr(recvMr.getAddr());
//			sgeRecv.setLength(recvMr.getLength());
//			int lkey = recvMr.getLkey();
//			sgeRecv.setLkey(lkey);
//			sgeListRecv.add(sgeRecv);
//			recvWR.setSg_list(sgeListRecv);
//			recvWR.setWr_id(2001);
//			wrList_recv.add(recvWR);

//			this.postRecv(wrList_recv).execute().free();
		}

		public void dispatchCqEvent(IbvWC wc) throws IOException {
			wcEvents.add(wc);
		}

		public LinkedBlockingQueue<IbvWC> getWcEvents() {
			return wcEvents;
		}

		public LinkedList<IbvSendWR> getWrList_send() {
			return wrList_send;
		}

		public LinkedList<IbvRecvWR> getWrList_recv() {
			return wrList_recv;
		}

        public LinkedList<IbvSge> getSgeListRecv() {
            return sgeListRecv;
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

		public IbvMr getSendMr() {
			return sendMr;
		}

		public IbvMr getRecvMr() {
			return recvMr;
		}
	}

}
