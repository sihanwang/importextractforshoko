package com.thomsonreuters.piers.ImportExportForShoko;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBConnections {
	public static final String URL = "jdbc:oracle:thin:@(description = (ADDRESS = (PROTOCOL = TCP)(HOST =  scan1.commodities.int.thomsonreuters.com)(PORT = 1521))(connect_data = (server = dedicated) (service_name = pocb.int.thomsonreuters.com)))";
	public static final String USERNAME = "cef_cnr";
	public static final String PASSWORD = "cef_cnr";
	
	private String url;
	private String username;
	private String password;
	
	public DBConnections(String url, String username, String password) {
		this.url = url;
		this.username = username;
		this.password = password;
	}
	
	public Connection getConn() throws Exception {
		Connection conn = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection(url, username, password);
            conn.setAutoCommit(false);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e.getMessage());
		}
        return conn;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
	
		
}

