package control_tool;


	import java.sql.Connection;
	import java.sql.PreparedStatement;
	import java.sql.ResultSet;
	import java.sql.SQLException;

import connection_utils.DBConnect;
	// Class dùng để lấy thông tin Config từ Database Control (table config)
	// Những thông tin ở class này được lấy ra để kết nối với Database Staging
	public class ConnectDBControl {
		public static String[] getConfigInformation() throws ClassNotFoundException, SQLException {
			String[] line = new String[14];
			Connection connDBControl = DBConnect.getConnection();
			String sql = "SELECT * FROM config WHERE id = (SELECT MAX(id) FROM config)";
			PreparedStatement ps = connDBControl.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				line[0] = Integer.toString(rs.getInt("id"));
				line[1] = rs.getString("class_forname");
				line[2] = rs.getString("port");
				line[3] = rs.getString("portnumber");
				line[4] = rs.getString("host");
				line[5] = rs.getString("user");
				line[6] = rs.getString("password");
				line[7] = rs.getString("db_name");
				line[8] = rs.getString("folder");
				line[9] = rs.getString("file");
				line[10] = rs.getString("filetype");
			
			}
			connDBControl.close();
			return line;
		}
		public static void main(String[] args) throws ClassNotFoundException, SQLException {
			String[] line = ConnectDBControl.getConfigInformation();
			for (int i = 0; i < line.length; i++) {
				System.out.println(line[i]);
			}
		}
	}

