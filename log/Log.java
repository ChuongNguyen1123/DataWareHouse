package log;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import connection_utils.DBConnect;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
public class Log {
	
	public static Timestamp getTimeStamp() {
		Date date = new Date();
		Timestamp ts = new Timestamp(date.getTime());
		return ts;
	}
	
	public static void createLog(int idConfig, String fileName, String fileType, String status) throws ClassNotFoundException, SQLException  {
		Connection conn = (Connection) DBConnect.getConnection();
		String sqlCreLog = "INSERT INTO tb_log VALUES(0, " + idConfig + ", '" + fileName + "', '" + fileType + "', current_timestamp(), '" + status + "')";
		PreparedStatement ps = (PreparedStatement) conn.prepareStatement(sqlCreLog);
		ps.executeUpdate();
		
	}
	
	public static void updateLog(int idLog, String status) throws ClassNotFoundException, SQLException {
		Connection conn =  (Connection) DBConnect.getConnection();
		String sqlUpdateLog = "UPDATE tb_log SET log_timestamp = current_timestamp(), file_status = '" + status + "' WHERE id_log = " + idLog + "";
		PreparedStatement ps = (PreparedStatement) conn.prepareStatement(sqlUpdateLog);
		ps.executeUpdate();
	
	}
	
	public static void main(String[] args) {
		System.out.println(getTimeStamp());
		
	}
}
