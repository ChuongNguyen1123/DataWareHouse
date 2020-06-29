package connection_utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBConnect {

	 private static  String URL="jdbc:mysql://localhost:3306/demo?useSSL=false";
	    private static String UNAME="root";
	    private  static String PASS ="Lananh300419@";
	static Connection con;
	   public static Connection getConnection() throws ClassNotFoundException, SQLException {
	        if (con != null && !con.isClosed()) {
	            return con;
	        } else {
	            Class.forName("com.mysql.jdbc.Driver");
	            con = DriverManager.getConnection(URL,UNAME,PASS);
	            return con;
	        }
	    }

	 
	    public static void main(String[] args) throws Exception {
	    	 String sql = "SELECT * FROM user";
	    	 PreparedStatement s = DBConnect.getConnection().prepareStatement(sql);
	        ResultSet rs = s.executeQuery(sql);
//	        int i=rs.
	        rs.last();
	        System.out.println(rs.getRow());
	        rs.beforeFirst();
	        while (rs.next()) {
	  
	            System.out.println(rs.getString(2));
	        }
	    	System.out.println("Kết nối thành công");
	    }

}
