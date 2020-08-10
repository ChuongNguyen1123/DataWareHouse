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

import download.SendMailSSL;

public class ConnectionDB {
	private String connectionURL, userName, passWord;
	private Connection connect;
	

//	Phuong thuc de ket noi DB. vd: ở class khac muon ket noi toi DB
// coppy     ConnectionDB connect = new ConnectionDB()
//	       Connection  connection = connect.loadProps();
//	todo......
	
//	phuong thuc load file dms_config.properties trong thu muc src
	public Connection loadProps() throws IOException {
		FileInputStream f = new FileInputStream("src/dms_config.properties");
		Properties pros = new Properties();
		pros.load(f);
		this.connectionURL = pros.getProperty("connectionURL");
		this.userName = pros.getProperty("userName");
		this.passWord = pros.getProperty("passWord");
		try {
			connect = DriverManager.getConnection(connectionURL, userName, passWord);
		} catch (SQLException e) {
			SendMailSSL senmail = new SendMailSSL(); 		// goi lop senmail
//			senmai (chu de mail, noi dung gui)
			senmail.sendMail("Data Warehouse nhom 12 - Ca sang ", " Khong the ket noi vao DB");
			System.out.println("Loi ket noi, kiem tra lai");  // in ra man hinh
			e.printStackTrace();
		}
//		System.out.println(connect);
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

	
//	public static void main(String[] args) throws IOException, SQLException {
//		new ConnectionDB().test(1);
//	}

}
