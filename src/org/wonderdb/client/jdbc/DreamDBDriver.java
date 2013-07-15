package org.wonderdb.client.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

public class DreamDBDriver implements Driver {

	@Override
	public boolean acceptsURL(String arg0) throws SQLException {
		if (arg0 != null && arg0.startsWith("wonderdb://")) {
			return true;
		}
		return false;
	}

	@Override
	public Connection connect(String arg0, Properties arg1) throws SQLException {
		arg0 = arg0.replaceAll("wonderdb://", "");
		String[] args = arg0.split(":");
		String host = args[0];
		int port = 6060;
		if (args.length == 2) {
			port = Integer.parseInt(args[1]);
		}
		return new DreamDBConnection(host, port);
	}

	@Override
	public int getMajorVersion() {
		return 0;
	}

	@Override
	public int getMinorVersion() {
		return 1;
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String arg0, Properties arg1)
			throws SQLException {
		return null;
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}
}
