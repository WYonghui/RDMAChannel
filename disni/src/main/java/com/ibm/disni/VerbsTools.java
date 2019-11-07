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

package com.ibm.disni;

import com.ibm.disni.rdma.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

public class VerbsTools {
    private static final Logger logger = LoggerFactory.getLogger(VerbsTools.class);
	private static final int MAX_ACK_COUNT = 4;
	private static final int POLL_CQ_LIST_SIZE = 16;

	public static boolean CachingON = false;

	private int ackCounter = 0;

	public static int MAX_SGE = 1;
	public static int MAX_WR = 100;

	private SVCPostSend postSendCall;
	private SVCPostRecv postRecvCall;
	private SVCReqNotify reqNotifyCall;
	private SVCPollCq pollCqCall;
	private IbvWC[] ibvWCs;

	private IbvQP qp;
	private IbvCompChannel compChannel;
	private IbvCQ cq;
	private final AtomicBoolean isStopped = new AtomicBoolean(false);

	public VerbsTools(IbvContext context, IbvCompChannel compChannel, IbvQP qp, IbvCQ cq) {
		this.postSendCall = null;
		this.postRecvCall = null;
		this.reqNotifyCall = null;
		this.pollCqCall = null;

		this.compChannel = compChannel;
		this.qp = qp;
		this.cq = cq;
	}

	public boolean send(ByteBuffer[] fragments,
			LinkedList<IbvSendWR> wrList, boolean signaled, boolean polling)
			throws Exception {
		for (int i = 0; i < fragments.length; i++) {
			ByteBuffer buffer = fragments[i];
			buffer.clear();
		}

		postSendCall = getPostSendCall(wrList);
		postSendCall.execute();
		if (signaled) {
			return checkCq(wrList.size(), polling);
		}

		return false;
	}

	public boolean send(LinkedList<IbvSendWR> wrList, boolean signaled, boolean polling)
			throws Exception {

		postSendCall = getPostSendCall(wrList);
		postSendCall.execute();

        logger.info("postSendCall execute");

		if (signaled) {
			return checkCq(wrList.size(), polling);
		}
		return false;
	}

	public void initSGRecv(LinkedList<IbvRecvWR> wrList)
			throws Exception {

		postRecvCall = getPostRecvCall(wrList);
		postRecvCall.execute();
	}

	public boolean completeSGRecv(LinkedList<IbvRecvWR> wrList, boolean polling) throws Exception {
		return checkCq(wrList.size(), polling);
	}

	public boolean completeSGSend(LinkedList<IbvSendWR> wrList, boolean polling) throws Exception {
		return checkCq(wrList.size(), polling);
	}


	public boolean checkCq(int expectedElements, boolean polling) throws Exception{
		boolean success = false;
		int elementsRead = 0;

		while (true) {
			if (!polling){
				reqNotifyCall = getReqNotifyCall();
				reqNotifyCall.execute();
			}

			pollCqCall = getPollCqCall(1);
			int res = pollCqCall.execute().getPolls();
			logger.info("checkCq res : {}" ,res);
			if (res < 0){
				logger.error("PollCQ failed executing with res: " + res);
				break;
			} else if (res > 0){
				for (int i = 0; i < res; i++) {
					boolean wcSuccess = ibvWCs[i].getStatus() == IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal();
					if (!wcSuccess) {
						logger.error("Completion with error: " +
								IbvWC.IbvWcStatus.values()[ibvWCs[i].getStatus()].name());
					}
				}

				if (ibvWCs[0].getStatus() == IbvWC.IbvWcStatus.IBV_WC_SUCCESS.ordinal()){
				} else {
					break;
				}
			}

			elementsRead += res;
			cq.ackEvents(res);
			if (elementsRead == expectedElements){
				success = true;
				break;
			} else {
				if (elementsRead > expectedElements){
					success = true;
					break;
				} else if (res == 0){
					if (!polling){
						compChannel.getCqEvent(cq, -1);
					}
				}
			}
		}
		logger.info("verbs checkCq : "+success);
		return success;
	}

	public void free(){
		if(postSendCall!=null) postSendCall.free();
		if(postRecvCall!=null) postRecvCall.free();
		if(pollCqCall!=null) pollCqCall.free();
		if(reqNotifyCall!=null) reqNotifyCall.free();
	}

	private SVCPostSend getPostSendCall(LinkedList<IbvSendWR> wrList) throws Exception{
		if (CachingON == false || this.postSendCall == null || !postSendCall.isValid()) {
			this.postSendCall = qp.postSend(wrList, null);
		}
		return postSendCall;
	}

	private SVCPostRecv getPostRecvCall(LinkedList<IbvRecvWR> wrList) throws Exception{
		if (CachingON == false || postRecvCall == null || !postRecvCall.isValid()) {
			postRecvCall = qp.postRecv(wrList, null);
		}
		return postRecvCall;
	}

	private SVCReqNotify getReqNotifyCall() throws Exception{
		if (CachingON == false || reqNotifyCall == null || !reqNotifyCall.isValid()) {
			reqNotifyCall = cq.reqNotification(false);
		}
		return reqNotifyCall;
	}

	private SVCPollCq getPollCqCall(int size) throws Exception{
		if (CachingON == false || pollCqCall == null || !pollCqCall.isValid()) {
			ibvWCs = new IbvWC[size];
			for (int i = 0; i < size; i++){
				ibvWCs[i] = new IbvWC();
			}
			pollCqCall = cq.poll(ibvWCs, size);
		}
		return pollCqCall;
	}
}

