package com.feeyo.raft.transport.server.handler;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.feeyo.net.codec.http.HttpRequest;
import com.feeyo.net.codec.protobuf.ProtobufDecoder;
import com.feeyo.net.codec.util.ProtobufUtils;
import com.feeyo.net.nio.util.TimeUtil;
import com.feeyo.raft.RaftServer;
import com.feeyo.raft.caller.callback.ProposeCallbackHttpRedirectImpl;
import com.feeyo.raft.proto.Raftpb;
import com.feeyo.raft.proto.Raftpb.Message;
import com.feeyo.raft.proto.Raftpb.Snapshot;
import com.feeyo.raft.transport.server.HttpConnection;
import com.feeyo.raft.transport.server.util.HttpUtil;
import com.feeyo.raft.util.UUIDUtil;

/**
 * Message RPC for Raft
 *  
 * @author zhuam
 *
 */
public class RaftMessageRequestHandler implements RequestHandler {
	
	private static Logger LOGGER = LoggerFactory.getLogger( "com.feeyo.raft.transport" );
	//
	private RaftServer raftSrv;
	//
	private static ProtobufDecoder<Message> protobufDecoder 
						= new ProtobufDecoder<Message>(Message.getDefaultInstance(), true);
	
	public RaftMessageRequestHandler(RaftServer raftSrv) {
		this.raftSrv = raftSrv;
	}

	@Override
	public String getHttpMethod() {
		return HTTP_POST;
	}

	@Override
	public String getPath() {
		return "/raft/message";
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
				List<Message> msgs = protobufDecoder.decode(request.getContent());
				if( msgs == null || raftSrv == null || !raftSrv.isRunning() ) {
					HttpUtil.sendOk(conn);
					return;
				}
				//
				long nowMillis = TimeUtil.currentTimeMillis();
				//
				boolean isIgnoreResponse = false;
				boolean isStep = true;
				for( Message msg : msgs ) {
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
							raftSrv.getCallbackRegistry().add( new ProposeCallbackHttpRedirectImpl(cbKey, conn, nowMillis) );
	
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
							raftSrv.saveSnapshotOfRemote(snapshot);
	
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
						raftSrv.step(conn, msg);
				}
	
			} catch (Throwable e) {
				LOGGER.error("raft request err: reqStr=" + request.toString(), e);
				HttpUtil.sendError(conn, e.getMessage());
			}
		}
	}

}