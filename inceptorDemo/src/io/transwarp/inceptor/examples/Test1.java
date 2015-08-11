package io.transwarp.inceptor.examples;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hive.jdbc.HiveConnection;


public class Test1 {

	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	private List<Map<String, String>> rows=new ArrayList<Map<String,String>>();
	
	public static void main(String[] args) throws IOException, SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		insert();
	}

	public static void insert() throws SQLException {
		Connection connection = null;
		PreparedStatement preparedStatement = null;
//		connection = HiveConnection.getHiveConnection();
		connection = DriverManager.getConnection("jdbc:transwarp://tdh2:10000/default");
		long l1 = System.currentTimeMillis();
		try {
			connection.setAutoCommit(false);
			String sql = "insert into batchinserttest(key,other_party,imsi) values(?,?,?)";
			preparedStatement = connection.prepareStatement(sql);
			for (int j = 1; j < 3; j++) {
				for (int i = 1; i <= 1000000; i++) {
					preparedStatement.setString(1, j+String.valueOf(i));
					preparedStatement.setInt(2, i);
					preparedStatement.setInt(3, i);
					preparedStatement.addBatch();
				}
				preparedStatement.executeBatch();
			}
			
//			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
	//		DbUtil.closeAll(connection, preparedStatement, null);
		}
		long l2 = System.currentTimeMillis();
		System.out.println(l2-l1);
	}

	class Reader extends Thread{
		
		File file;
		public Reader(File file){
			this.file=file;
		}
		@Override
		public void run() {
			try {
				FileInputStream fis = new FileInputStream(file);
				BufferedReader br = new BufferedReader(new InputStreamReader(fis,
						"UTF-8"));
			} catch (Exception e) {
			}
			
		}
	}
}
