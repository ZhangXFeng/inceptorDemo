package io.transwarp.inceptor.examples;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.DriverManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HiveJdbcTest {
	private static Log LOG = LogFactory.getLog(HiveJdbcTest.class);
	// private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	// //hiveserver2
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static String url = "jdbc:transwarp://tdh2:10000/default";
	static {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
			System.exit(1);
		}
	}

	static void createTable() {
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(url, "", "");
			Statement st = connection.createStatement();
			String sql1 = "set transaction.type=inceptor";
			String sql2 = "drop table if exists acidTable";
			String sql3 = "drop table if exists partitionAcidTable";
			String sql4 = "drop table if exists range_int_table";
			String sql5 = "create table acidTable(name varchar(10),age int, degree int) clustered by (age) into 10 buckets stored as orc TBLProperties(\"transactional\"=\"true\")";
			String sql6 = "create table partitionAcidTable(name varchar(10),age int) partitioned by (city string) clustered by (age) into 10 buckets stored as orc TBLProperties(\"transactional\"=\"true\")";
			String sql7 = "create table range_int_table(id int, value int) partitioned by range (id) (partition less1 values less than (1), partition less10 values less than (10) ) clustered by (value) into 3 buckets stored as orc TBLProperties(\"transactional\"=\"true\")";
			st.execute(sql1);
			st.execute(sql2);
			st.execute(sql3);
			st.execute(sql4);
			st.execute(sql5);
			st.execute(sql6);
			st.execute(sql7);

		} catch (SQLException e) {
			LOG.error(e.getMessage(), e);
		} finally {
			closeConnection(connection);
		}

	}

	private static void closeConnection(Connection connection) {
		if (null != connection) {
			try {
				connection.close();
			} catch (SQLException e) {
				LOG.error("connection close error", e);
			}
		}
	}

	static void insert() {
		String sql1="Insert into acidTable(name, age,degree) values ('aaa', 12,23)";
		String sql2="Insert into partitionAcidTable partition(city='sh') (name, age)values ('aaa', 12)";
		String sql3 = "set transaction.type=inceptor";
		Connection conn=null;
		try {
			conn = DriverManager.getConnection(url, "", "");
			Statement st=conn.createStatement();
			st.execute(sql3);
			st.execute(sql1);
			st.execute(sql2);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}finally{
			closeConnection(conn);
		}
	}

	static void batchInsert() {
		String sql1="set transaction.type=inceptor";
		String sql2="Batchinsert into  acidTable(name, age,degree) batchvalues(values ('aaa', 12,3), values ('bbb', 22,9))";
		String sql3="Batchinsert into  partitionAcidTable partition(city='sh') (name, age) batchvalues(values ('aaa', 12), values ('bbb', 22))";
		Connection conn=null;
		try {
			conn = DriverManager.getConnection(url, "", "");
			Statement st=conn.createStatement();
			
			st.execute(sql1);
			st.execute(sql2);
			st.execute(sql3);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}finally{
			closeConnection(conn);
		}
	}
	
	static void update(){
		String sql1="set transaction.type=inceptor";
		String sql2="update acidTable set degree=degree*2 where age>15";
		Connection conn=null;
		try {
			conn = DriverManager.getConnection(url, "", "");
			Statement st=conn.createStatement();
			
			st.execute(sql1);
			st.execute(sql2);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}finally{
			closeConnection(conn);
		}
	}
	
	static void delete(){
		String sql1="set transaction.type=inceptor";
		String sql2="delete from acidTable where name='bbb'";
		Connection conn=null;
		try {
			conn = DriverManager.getConnection(url, "", "");
			Statement st=conn.createStatement();
			
			st.execute(sql1);
			st.execute(sql2);
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}finally{
			closeConnection(conn);
		}
	}

	public static void main(String[] args) throws SQLException {

//		createTable();
//		batchInsert();
//		update();
		delete();

		

	}

}
