package data_warehouse;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionDB1 {
	private String connectionURL1, userName1, passWord1;
	private Connection connect;
	
//ConnectionDB1 connect to Staging
//	Phuong thuc de ket noi DB. vd: ở class khac muon ket noi toi DB
// coppy     ConnectionDB connect = new ConnectionDB()
//	       Connection  connection = connect.loadProps();
//	todo......
	
//	phuong thuc load file dms_config.properties trong thu muc src
	public Connection loadProps() throws IOException, SQLException, ClassNotFoundException {
		FileInputStream f = new FileInputStream("src/data_warehouse/dms_config.properties");
		Properties pros = new Properties();
		pros.load(f);
		this.connectionURL1 = pros.getProperty("connectionURL1");
		this.userName1 = pros.getProperty("userName1");
		this.passWord1 = pros.getProperty("passWord1");
		Class.forName("com.mysql.cj.jdbc.Driver");
		connect = DriverManager.getConnection(connectionURL1, userName1, passWord1);
//		System.out.println(connect);
		return connect;
	}
	
	

	public String getConnectionURL1() {
		return connectionURL1;
	}

	public String getUserName1() {
		return userName1;
	}

	public String getPassWord1() {
		return passWord1;
	}


}
