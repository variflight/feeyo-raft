package com.feeyo.raft.transport.client;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.feeyo.net.nio.ClosableConnection;
import com.feeyo.net.nio.NetSystem;

public class ConQueue {

	//连接
	private final ConcurrentLinkedQueue<HttpClientConnection> cons = new ConcurrentLinkedQueue<HttpClientConnection>();

	public HttpClientConnection takeIdleCon() {		
		ConcurrentLinkedQueue<HttpClientConnection> f1 = cons;
		HttpClientConnection con = f1.poll();
		if (con == null || con.isClosed() || !con.isConnected() ) {
			return null;
		} else {
			return con;
		}
	}

	public ConcurrentLinkedQueue<HttpClientConnection> getCons() {
		return cons;
	}

	public ArrayList<HttpClientConnection> getIdleConsToClose(int count) {
		ArrayList<HttpClientConnection> readyCloseCons = new ArrayList<HttpClientConnection>(count);
		while (!cons.isEmpty() && readyCloseCons.size() < count) {
			HttpClientConnection theCon = cons.poll();
			if (theCon != null) {
				readyCloseCons.add(theCon);
			}
		}
		return readyCloseCons;
	}	
	
	public int getActiveCountForNode(PhysicalNode node) {
        int total = 0;
        for (ClosableConnection conn : NetSystem.getInstance().getAllConnectios().values()) {
            if (conn instanceof HttpClientConnection) {
            	HttpClientConnection theCon = (HttpClientConnection) conn;
                if (theCon.getPhysicalNode() == node) {
                    if (theCon.isBorrowed()) {
                        total++;
                    }
                }
            }
        }
        return total;
    }

    public void clearConnections(String reason, PhysicalNode node) {
        Iterator<Entry<Long, ClosableConnection>> itor = 
        		NetSystem.getInstance().getAllConnectios().entrySet().iterator();
        while ( itor.hasNext() ) {
            Entry<Long, ClosableConnection> entry = itor.next();
            ClosableConnection con = entry.getValue();
            if (con instanceof HttpClientConnection) {
                if (((HttpClientConnection) con).getPhysicalNode() == node) {
                    con.close(reason);
                    itor.remove();
                }
            }
        }
    }
    
    public void setIdleTimeConnections(PhysicalNode node, long idleTimeout) {    	
        Iterator<Entry<Long, ClosableConnection>> itor = 
        		NetSystem.getInstance().getAllConnectios().entrySet().iterator();
        while ( itor.hasNext() ) {
            Entry<Long, ClosableConnection> entry = itor.next();
            ClosableConnection con = entry.getValue();
            if (con instanceof HttpClientConnection) {
                if (((HttpClientConnection) con).getPhysicalNode() == node) {
                	con.setIdleTimeout( idleTimeout );
                }
            }
        }
    }
}