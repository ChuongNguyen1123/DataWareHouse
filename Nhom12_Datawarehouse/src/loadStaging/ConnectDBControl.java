package loadStaging ;


	import java.io.IOException;
import java.sql.Connection;
	import java.sql.PreparedStatement;
	import java.sql.ResultSet;
	import java.sql.SQLException;

import connectionDB.ConnectionDB;

	// Class dùng để lấy thông tin Config từ Database Control (table config)
	// Những thông tin ở class này được lấy ra để kết nối với Database Staging
	public class ConnectDBControl {
		public static String[] getConfigInformation() throws ClassNotFoundException, SQLException, IOException {
			String[] line = new String[14];
			ConnectionDB connect = new ConnectionDB();
		       Connection  connection = connect.loadProps();
			String sql = "SELECT * FROM table_config ";
			PreparedStatement ps = connection.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				line[0] = Integer.toString(rs.getInt("id"));
				line[1] = rs.getString("url");
				line[2] = rs.getString("username");
				line[3] = rs.getString("password");
				line[4] = rs.getString("source_folder");
				line[5] = rs.getString("folder_local");
				line[6] = rs.getString("file");
				line[7] = rs.getString("filetype");
				line[8] = rs.getString("staging_table");
				line[9] = rs.getString("filed_name");
				line[10] = rs.getString("number_cols");
		
			
			
			}
			connection.close();
			return line;
		}
		public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
			String[] line = ConnectDBControl.getConfigInformation();
			for (int i = 0; i < line.length; i++) {
				System.out.println(line[i]);
				
			
			}
		}
	}
	
