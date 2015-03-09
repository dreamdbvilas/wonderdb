package org.wonderdb.query.sql;
/*******************************************************************************
 *    Copyright 2013 Vilas Athavale
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class WonderDBDriver implements Driver {

	public WonderDBDriver() {
		// TODO Auto-generated constructor stub
		int i = 0;
	}
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
		Connection c = WonderDBConnectionPool.getInstance().getConnection(host, port);
		return c != null ? c : new WonderDBConnection(host, port);
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

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		// TODO Auto-generated method stub
		return null;
	}
}
