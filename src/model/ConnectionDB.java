package model;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionDB {
	private String urlHttp, usernameDriver, passwordDriver, srcFolderDriver, desSRCLocal, kieuFile;
	private String connectionURL, userName, passWord, sql;

	public Connection loadProps() throws IOException {
		Connection result = null;
		FileInputStream f = new FileInputStream("src/model/dms_config.properties");
		Properties pros = new Properties();
		pros.load(f);
		this.connectionURL = pros.getProperty("connectionURL");
		this.userName = pros.getProperty("userName");
		this.passWord = pros.getProperty("passWord");
		this.sql = pros.getProperty("sqlconfig");
		try {
			result = DriverManager.getConnection(connectionURL, userName, passWord);
			String lenhSQL = sql;
			PreparedStatement pre = result.prepareStatement(sql);
			ResultSet resultSet = pre.executeQuery();
			while(resultSet.next()) {
				urlHttp = resultSet.getString("url");
				usernameDriver = resultSet.getString("username");
				passwordDriver = resultSet.getString("password");
				srcFolderDriver = resultSet.getString("source_folder");
				desSRCLocal = resultSet.getString("folder_local");
				kieuFile = resultSet.getString("file");
			}
			
		} catch (SQLException e) {
			System.out.println("Loi ket noi, kiem tra lai");
			System.exit(0);
			e.printStackTrace();
		}
		System.out.println(urlHttp + " ; " + usernameDriver );
		return result;
	}

	public static void main(String[] args) throws IOException {
		ConnectionDB connectionDB = new ConnectionDB();
		System.out.println(connectionDB.loadProps());
	}

}
