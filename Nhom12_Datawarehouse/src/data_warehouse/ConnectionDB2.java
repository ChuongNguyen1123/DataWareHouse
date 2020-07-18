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

public class ConnectionDB2 {
	private String connectionURL2, userName2, passWord2;
	private Connection connect;

	// ConnectionDB1 connect to Staging
	// Phuong thuc de ket noi DB. vd: ở class khac muon ket noi toi DB
	// coppy ConnectionDB connect = new ConnectionDB()
	// Connection connection = connect.loadProps();
	// todo......

	// phuong thuc load file dms_config.properties trong thu muc src
	public Connection loadProps() throws IOException, SQLException, ClassNotFoundException {
		FileInputStream f = new FileInputStream("src/data_warehouse/dms_config.properties");
		Properties pros = new Properties();
		pros.load(f);
		this.connectionURL2 = pros.getProperty("connectionURL2");
		this.userName2 = pros.getProperty("userName2");
		this.passWord2 = pros.getProperty("passWord2");
		Class.forName("com.mysql.cj.jdbc.Driver");
		connect = DriverManager.getConnection(connectionURL2, userName2, passWord2);
		// System.out.println(connect);
		return connect;
	}

	public String getConnectionURL2() {
		return connectionURL2;
	}

	public String getUserName2() {
		return userName2;
	}

	public String getPassWord2() {
		return passWord2;
	}



}
