package connectionDB;

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
	private String connectionURL, userName, passWord;
	private Connection connect;
	

//	Phuong thuc de ket noi DB. vd: ở class khac muon ket noi toi DB
// coppy     ConnectionDB connect = new ConnectionDB()
//	       Connection  connection = connect.loadProps();
//	todo......
	
//	phuong thuc load file dms_config.properties trong thu muc src
	public Connection loadProps() throws IOException, SQLException {
		FileInputStream f = new FileInputStream("src/dms_config.properties");
		Properties pros = new Properties();
		pros.load(f);
		this.connectionURL = pros.getProperty("connectionURL");
		this.userName = pros.getProperty("userName");
		this.passWord = pros.getProperty("passWord");
		connect = DriverManager.getConnection(connectionURL, userName, passWord);
		System.out.println(connect);
		return connect;
	}
	
	

	public String getConnectionURL() {
		return connectionURL;
	}

	public String getUserName() {
		return userName;
	}

	public String getPassWord() {
		return passWord;
	}

	
	public static void main(String[] args) throws IOException, SQLException {
		System.out.println(new ConnectionDB());
	}

}
