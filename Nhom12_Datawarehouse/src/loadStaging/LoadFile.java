package loadStaging ;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.xmlbeans.impl.xb.xmlconfig.ConfigDocument.Config;

import connectionDB.ConnectionDB;

//import Check.Check;
//import connection_utils.DBConnect;
//import control_tool.ConnectDBControl;
//import control_tool.config;
//import log.Log;
//import log.LogSTT;
//import tool.ExchangeJavaPath;

public class LoadFile {
	
	public boolean createTable(String table_name, String column_list) throws ClassNotFoundException, SQLException, IOException {
        ConnectionDB connect = new ConnectionDB();
	       Connection  connection = connect.loadProps();
		String sql = "CREATE TABLE "+table_name+" (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,";
		String[] col = column_list.split(",");
		
		for(int i =0;i<col.length;i++) {
			sql+=col[i]+" "+"varchar(100)"+ " NOT NULL,";
		}
		sql = sql.substring(0,sql.length()-1)+")";
		System.out.println(sql);
		try {
			
			PreparedStatement s = connection.prepareStatement(sql);
			s.executeUpdate();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		

		}
		
		
	}
	public void LoadFile1(String table_name,List<SinhVien> listBooks) throws ClassNotFoundException, SQLException, IOException {
		String[] line = ConnectDBControl.getConfigInformation();	
		String file1 = line[5] + "\\" + line[6];
		   ConnectionDB connect = new ConnectionDB();
	       Connection  connection = connect.loadProps();
//		String  dsSinhVien = ReadFileExcel.readFile(file1);	
		String sql = "INSERT INTO " +table_name+ " (stt,maSV,hoLot,ten,ngaySinh,maLop,tenLop,dtLienLac,email,queQuan,ghiChu)  VALUES(?,?,?,?,?,?,?,?,?,?,?) " ;
		PreparedStatement ps = connection.prepareStatement(sql);
		for (SinhVien book : listBooks) {
			 ps.setString(1, book.getStt());
			 ps.setString(2, book.getMaSV());
			 ps.setString(3, book.getHoLot());
			 ps.setString(4, book.getTen());
			 ps.setString(5, book.getNgaySinh());
			 ps.setString(6, book.getMaLop());
			 ps.setString(7, book.getTenLop());
			 ps.setString(8, book.getDtLienLac());
			 ps.setString(9, book.getEmail());
			 ps.setString(10, book.getQueQuan());
			 ps.setString(11, book.getGhiChu());
			 ps.addBatch();
			 
		
			
}
		ps.executeBatch();
		ps.close();
		System.out.println("Insert thành công");
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		String[] line = ConnectDBControl.getConfigInformation();	
		String file1 = line[5] + "\\" + line[6];	
		String[] line1=SinhVien.getConfigInformation1();
		LoadFile lf = new LoadFile();
		config cn = new config();
		String table_name = line[8];
		String column_list = line[9];
		String value = line1[1] + line1[2]+ line1[3] + line1[4] + line1[5] + line1[6] + line1[7] + line1[8] + line1[9] + line1[10] + line1[11];
//		String value = ReadFileExcel.readFile("C:\\Users\\Admin\\Documents\\17130016_sang_nhom12.xlsx");
		List<SinhVien> listBooks = new ReadFileExcel().readBooksFromExcelFile(file1);
		lf.LoadFile1(table_name,listBooks);
//		System.out.println(value);
//		System.out.println(lf.createTable(table_name, column_list));
	
		
	}
	
	}


