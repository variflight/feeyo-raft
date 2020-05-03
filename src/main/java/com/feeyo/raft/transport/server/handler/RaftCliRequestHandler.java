package com.feeyo.raft.transport.server.handler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.feeyo.net.codec.http.HttpRequest;
import com.feeyo.net.codec.http.HttpResponseStatus;
import com.feeyo.net.codec.http.UriUtil;
import com.feeyo.raft.RaftServer;
import com.feeyo.raft.Peer;
import com.feeyo.raft.Progress;
import com.feeyo.raft.Progress.ProgressState;
import com.feeyo.raft.ProgressSet;
import com.feeyo.raft.proto.Raftpb.ConfChange;
import com.feeyo.raft.proto.Raftpb.ConfChangeType;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.util.MessageUtil;
import com.feeyo.raft.transport.server.HttpConnection;
import com.feeyo.raft.transport.server.util.HttpUtil;
import com.google.protobuf.ByteString;

// Client CommandLine Service
//
public class RaftCliRequestHandler implements RequestHandler {
	//
	private RaftServer raftSrv;
	
	public RaftCliRequestHandler(RaftServer raftSrv) {
		this.raftSrv = raftSrv;
	}
	
	@Override
	public String getHttpMethod() {
		return HTTP_POST;
	}

	@Override
	public String getPath() {
		return "/raft/cli";
	}
	
	@Override
	public void handle(HttpConnection conn, List<HttpRequest> requestList) {
		
		try {
			String queryStr = requestList.get(0).getUri();
			Map<String, String> paramters = UriUtil.parseParameters( queryStr );
			if ( !paramters.isEmpty() ) {
				//
				String cmd = paramters.get("cmd");
				if ( cmd != null ) {
					// 查询节点信息
					if ( cmd.equalsIgnoreCase("getNodes") ) {
						
						Map<String, Object> responseMap = new HashMap<String, Object>();
						
						List<Map<String,Object>> data = new ArrayList<Map<String, Object>>();
						
						long leaderId = raftSrv.getLeaderId();
						for(Peer peer: raftSrv.getPeerSet().values()) {
							Map<String, Object> item = new HashMap<String, Object>();
							item.put("id", peer.getId());
							item.put("ip", peer.getIp());
							item.put("port", peer.getPort());
							item.put("state" , leaderId != 0 && leaderId == peer.getId() ? "LEADER" : "FOLLOWER");
							data.add( item );
						}
						
						responseMap.put("code", 200);
						responseMap.put("data", data);
						
						String txt = JSON.toJSONString( responseMap );
						HttpUtil.send(conn, HttpResponseStatus.OK, txt.getBytes());
						return;
					
					// 查询节点执行进度
					} else if ( cmd.equalsIgnoreCase("getNodePrs") ) {
						
						Map<String, Object> responseMap = new HashMap<String, Object>();
						
						List<Map<String,Object>> data = new ArrayList<Map<String, Object>>();
						
						ProgressSet prs = raftSrv.getPrs();
						HashMap<Long, Progress> prMap = new HashMap<Long, Progress>();
						prMap.putAll( prs.voters() );
						prMap.putAll( prs.learners() );
						//
						for(Map.Entry<Long, Progress> voter: prMap.entrySet()) {
							long id = voter.getKey();
							Progress progress = voter.getValue();
							
							Map<String, Object> item = new HashMap<String, Object>();
							item.put("id", id);
							item.put("matched", 		progress.getMatched());
							item.put("nextIndex", 		progress.getNextIdx());
							item.put("pendingSnapshot", progress.getPendingSnapshot());
							item.put("isRecentActive", 	progress.isRecentActive());
							item.put("isLearner", 		progress.isLearner());
							item.put("isPaused", 		progress.isPaused());
							item.put("state" ,  		ProgressState.toString( progress.getState() ) );
							
							data.add( item );
						}
						
						responseMap.put("code", 200);
						responseMap.put("data", data);
						
						String txt = JSON.toJSONString( responseMap  );
						HttpUtil.send(conn, HttpResponseStatus.OK, txt.getBytes());
						return;
					
					// 增加新节点
					} else if ( cmd.equalsIgnoreCase("addNode")  ) {
						
						//
						int id = Integer.parseInt( paramters.get("id") );
						String ip = paramters.get("ip");
						int port = Integer.parseInt( paramters.get("port") );
						boolean isLeaner = Boolean.parseBoolean( paramters.get("isLeaner") );
						
						Peer.Endpoint endpoint = new Peer.Endpoint(ip, port);
						
						ConfChange cc = ConfChange.newBuilder() //
								.setChangeType( isLeaner ? ConfChangeType.AddLearnerNode: ConfChangeType.AddNode) //
								.setNodeId(id)
								.setContext(ByteString.copyFromUtf8(endpoint.toString()))
								.build();
						
						Message msg =  MessageUtil.proposeConfChange(cc);
						this.localStep(conn, msg);
		
					// 移除节点
					} else if ( cmd.equalsIgnoreCase("removeNode") ) {
						
						int id = Integer.parseInt( paramters.get("id") );
						
						ConfChange cc = ConfChange.newBuilder()
								.setChangeType(ConfChangeType.RemoveNode)
								.setNodeId(id)
								.build();
						
						Message msg =  MessageUtil.proposeConfChange(cc);
						this.localStep(conn, msg);
					
					// 转移 leader
					} else if ( cmd.equalsIgnoreCase("transferLeader") ) {
						//
						int id = Integer.parseInt( paramters.get("id") );
						// 1、如果当前的raft.leadTransferee成员不为空，说明有正在进行的leader迁移流程。
						// 此时会判断是否与这次迁移是同样的新leader ID，如果是则忽略该消息直接返回；否则将终止前面还没有完毕的迁移流程
						// 2、如果这次迁移过去的新节点，就是当前的leader ID，也直接返回不进行处理
						// 3、
						
						Message msg = MessageUtil.transferLeader(id);
						this.localStep(conn, msg);
					} 
					
				}  else {
					Map<String, Object> responseMap = new HashMap<String, Object>();
					responseMap.put("code", 404);
					responseMap.put("data", "Error# unknow cmd=" + cmd + ", queryStr=" + queryStr + " !");
		
					String txt = JSON.toJSONString( responseMap  );
					HttpUtil.send(conn, HttpResponseStatus.OK, txt.getBytes());
				}
				
			} else {
				// Bed request
				HttpUtil.send(conn, HttpResponseStatus.BAD_REQUEST);
			}
		} catch(Throwable e) {
			HttpUtil.send(conn, HttpResponseStatus.BAD_REQUEST);
		}
	}
	
	//
	private void localStep(HttpConnection conn, Message msg) {
		
		Map<String, Object> responseMap = new HashMap<String, Object>();
		
		// not leader
		long leaderId = raftSrv.getLeaderId();
		if ( leaderId <= 0 ) {
			responseMap.put("code", 500);
			responseMap.put("data", "Error# not leader !");
			
		} else {
			raftSrv.local_step(null, msg);
			responseMap.put("code", 200);
			responseMap.put("data", "success !");
		}
		//
		String txt = JSON.toJSONString( responseMap  );
		HttpUtil.send(conn, HttpResponseStatus.OK, txt.getBytes());
	}

}