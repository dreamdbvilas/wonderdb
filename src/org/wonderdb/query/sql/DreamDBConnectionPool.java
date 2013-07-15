package org.wonderdb.query.sql;

import java.sql.Connection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public class DreamDBConnectionPool {
	private static DreamDBConnectionPool instance = new DreamDBConnectionPool();
	ConcurrentMap<String, ConcurrentLinkedQueue<Connection>> map = new ConcurrentHashMap<String, ConcurrentLinkedQueue<Connection>>();
	
	private DreamDBConnectionPool() {
	}
	
	public static DreamDBConnectionPool getInstance() {
		return instance;
	}
	
	public Connection getConnection(String host, int port) {
		String key = host + "_" + port;
		ConcurrentLinkedQueue<Connection> q = map.get(key);
		ConcurrentLinkedQueue<Connection> q1  = q;
		if (q == null) {
			q = new ConcurrentLinkedQueue<Connection>();
			q1 = map.putIfAbsent(key, q);
		}
		if (q1 == null) {
			q1 = q;
		}
		
		Connection c = q1.poll();
		return c;
	}
	
	public void returnConnection(DreamDBConnection c) {
		String key = c.host + "_" + c.port;
		ConcurrentLinkedQueue<Connection> q = map.get(key);
		q.add(c);
	}
	
	public void shutdown() {
		Iterator<ConcurrentLinkedQueue<Connection>> iter = map.values().iterator();
		while (iter.hasNext()) {
			ConcurrentLinkedQueue<Connection> q = iter.next();
			if (q == null) {
				continue;
			}
			Iterator<Connection> cIter = q.iterator();
			while (cIter.hasNext()) {
				DreamDBConnection c = (DreamDBConnection) cIter.next();
				c.shutdown();
			}
		}
	}
}
