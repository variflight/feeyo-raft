package com.feeyo.raft.transport.server.handler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.http.HttpRequest;
import com.feeyo.net.codec.protobuf.ProtobufDecoder;
import com.feeyo.net.codec.util.ProtobufUtils;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.caller.callback.ProposeCallbackHttpRedirectImpl;
import com.feeyo.raft.group.RaftGroup;
import com.feeyo.raft.group.RaftGroupServer;
import com.feeyo.raft.group.proto.Raftgrouppb.RaftGroupMessage;
import com.feeyo.raft.proto.Raftpb;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.transport.server.HttpConnection;
import com.feeyo.raft.transport.server.util.HttpUtil;
import com.feeyo.raft.util.UUIDUtil;

/**
 *  Message RPC for Multi-raft
 * 
 * @author zhuam
 *
 */
public class RaftGroupMessageRequestHandler implements RequestHandler {
	//
	private static Logger LOGGER = LoggerFactory.getLogger("com.feeyo.raft.transport");
	//
	private static ProtobufDecoder<RaftGroupMessage> protobufDecoder = new ProtobufDecoder<RaftGroupMessage>(
			RaftGroupMessage.getDefaultInstance(), true);
	//
	private RaftGroupServer raftGroupSrv;
	
	//
	public RaftGroupMessageRequestHandler(RaftGroupServer raftGroupSrv) {
		this.raftGroupSrv = raftGroupSrv;
	}

	@Override
	public String getHttpMethod() {
		return HTTP_POST;
	}

	@Override
	public String getPath() {
		return "/raft/group/message";
	}

	//
	@Override
	public void handle(final HttpConnection conn, final List<HttpRequest> requestList) {
		// 
		//
		for(HttpRequest request: requestList) {
			//
			try {
				//
				List<RaftGroupMessage> gMsgs = protobufDecoder.decode(request.getContent());
				if( gMsgs == null || raftGroupSrv == null || !raftGroupSrv.isRunning() ) {
					HttpUtil.sendOk(conn);
					return;
				}
				//
				long nowMillis = TimeUtil.currentTimeMillis();
				//
				boolean isIgnoreResponse = false;
				boolean isStep = true;
				for( RaftGroupMessage gMsg : gMsgs ) {
					//
					long regionId = gMsg.getRegionId();
					Message msg = gMsg.getMessage();
					//
					final RaftGroup raftGroup  = raftGroupSrv.findGroupByRegionId(regionId);
					if ( raftGroup == null ) {
						LOGGER.error("The region {} was not found in the multi-raft", regionId);
						continue;
					}
					//
					//
					switch( msg.getMsgType() ) {
						case MsgPropose: {
							//
							if ( LOGGER.isDebugEnabled()  )
								LOGGER.debug("#recv host={}, protobuf={}", conn.getHost(), ProtobufUtils.protoToJson( msg ) );
	
							// TODO: 注意事项
							// 1、当前用 last entry 的 data 做为 propose 回调的 key ，并不是最好的选择
							// 2、Follower forward MsgPropose to leader , must be wait and hold connection
							//
							// 注入 callback key
							String cbKey = UUIDUtil.getUuid();
							//
							List<Raftpb.Entry> entries = msg.getEntriesList();
							int lastIndex = entries.size() - 1;
							//
							Raftpb.Entry.Builder entBuilder = entries.get( lastIndex ).toBuilder();
							entBuilder.setCbkey( cbKey );
							//
							Raftpb.Message.Builder msgBuilder = msg.toBuilder();
							msgBuilder.setEntries(lastIndex, entBuilder.build());
							msg = msgBuilder.build();
							
							//
							raftGroup.getCallbackRegistry().add( new ProposeCallbackHttpRedirectImpl(cbKey, conn, nowMillis) );
	
							// ignore response
							isIgnoreResponse = true;
	
							break;
						}
						case MsgSnapshot: {
							//
							if ( LOGGER.isDebugEnabled()  )
								LOGGER.debug("#recv host={}, protobuf={}", conn.getHost(), 
										ProtobufUtils.protoToJson(  msg.getSnapshot().getMetadata() ) );
	
							// 存储远程快照
							Snapshot snapshot = msg.getSnapshot();
							raftGroup.saveSnapshotOfRemote(snapshot);
	
							// 如果最后一个数据块没有过来，不往Raft内传递
							isStep = snapshot.getChunk().getLast();
							break;
						}
						case MsgHeartbeat:
						case MsgHeartbeatResponse:
						case MsgReadIndex:
						case MsgReadIndexResp:
							//
							if ( LOGGER.isDebugEnabled()  )
								LOGGER.debug("#recv host={}, protobuf={}", conn.getHost(), ProtobufUtils.protoToJson( msg ) );
							break;
						case MsgAppend:
							// ignore log
							break;
						case MsgRequestPreVote:
						case MsgRequestPreVoteResponse:
						case MsgRequestVote:
						case MsgRequestVoteResponse:
							// 投票相关日志，输出
							LOGGER.info("#recv host={}, protobuf={}", conn.getHost(), ProtobufUtils.protoToJson(  msg ) );
							break;
						default:
							if ( LOGGER.isDebugEnabled()  )
								LOGGER.debug("#recv host={}, protobuf={}", conn.getHost(), ProtobufUtils.protoToJson(  msg ) );
							break;
					}
					//
					if ( !isIgnoreResponse ) {
						isIgnoreResponse = true;
						HttpUtil.sendOk(conn);
					}
					//
					if ( isStep )
						raftGroup.step(conn, msg);
				}
	
			} catch (Throwable e) {
				LOGGER.error("raft-group request err: reqStr=" + request.toString(), e);
				HttpUtil.sendError(conn, e.getMessage());
			}
		}
	}

}