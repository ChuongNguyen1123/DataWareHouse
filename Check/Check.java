package Check;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import connection_utils.DBConnect;

public class Check {
	public static boolean checkHaveData(String tbName) throws ClassNotFoundException, SQLException {
		boolean rs = false;
		Connection conn = DBConnect.getConnection();
		String sql = "SELECT * FROM " + tbName + "";
		PreparedStatement ps = conn.prepareStatement(sql);
		ResultSet rSet = ps.executeQuery();
		while (rSet.next()) {
			System.out.println("Loaded data successfully!");
			rs = true;
			break;
		}
		conn.close();
		return rs;
	}
	
	public static boolean checkSum(String tbName, int numRecords) throws ClassNotFoundException, SQLException {
		boolean rs = false;
		Connection connection = DBConnect.getConnection();
		Statement s = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		String sql = "SELECT * FROM " + tbName + "";
		ResultSet rSet = s.executeQuery(sql);
		rSet.last();
		int records = rSet.getRow();
		rSet.beforeFirst();
		
		System.out.println(records);
		if (records == numRecords) {
			System.out.println("LOADED DATA SUCCESSFULLY!");
			rs = true;
		}
		connection.close();
		return rs;
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		System.out.println(checkSum("tb_staging", 38));
	}
}
