package loadStaging ;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Scanner;
import java.util.Date;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.xmlbeans.impl.xb.xmlconfig.ConfigDocument.Config;

import connectionDB.ConnectionDB;
import download.SendMailSSL;


public class LoadFile {
	public static int checkTableExist(String table_name, String column_list,String stt) throws IOException, SQLException, ClassNotFoundException {
		LoadFile ll = new LoadFile();
		 ConnectionDB connect = new ConnectionDB();
	       Connection  connection = connect.loadProps();
		
		String sql = "select count(table_name) from  information_schema.`TABLES` where table_name ='"+table_name+"'";
		PreparedStatement statement = null;
		ResultSet res = null;
		try { 
			PreparedStatement ps = connection.prepareStatement(sql);
			res = ps.executeQuery();
			while (res.next()) {
				return res.getInt(1);
				
			
			}
		} catch (SQLException e) {
		
			e.printStackTrace();
		} finally {
			try {
				if (res.getInt(1)==1) {
					res.close();
					System.out.println("Không thể tạo bảng");
					System.out.println("Bảng đã tồn tại");
				}else if(res.getInt(1)==0) {
					res.close();
					ll.createTable(table_name, column_list, stt);
					System.out.println("Tạo bảng thành công");
				}
				
			} catch (SQLException e) {
	
				e.printStackTrace();
			}
		}
		return 0;
	}

	public boolean createTable(String table_name, String column_list,String stt) throws ClassNotFoundException, SQLException, IOException {
		ConnectionDB connect = new ConnectionDB();
	       Connection  connection = connect.loadProps();
	      
		String sql = "CREATE TABLE database_staging."+table_name+" (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,";
		String[] col = column_list.split(",");
		for(int i =0;i<col.length;i++) {
			sql+=col[i]+" "+"varchar(100)"+ "  NULL,";
		}
		sql = sql.substring(0,sql.length()-1)+")";
		System.out.println(sql);
		try {
			if(stt.equals("Download_OK")) {
			PreparedStatement s = connection.prepareStatement(sql);
			s.executeUpdate();
			return true;
			}
			else {
				if(stt.equals("Download_Fail")) {
					System.out.println("Không thể tạo bảng");
			}
		}
		}catch (SQLException e) {
			e.printStackTrace();


		}
		return false;
		
		
	}

public static String getTime() {
DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
Date date = new Date();
System.out.println(df.format(date));
	return df.format(date);
	
}

public static boolean updateLog(String  status,String date_staging,String id1) throws IOException, SQLException {
	  ConnectionDB connect = new ConnectionDB();
      Connection  connection = connect.loadProps();
      String sql = "UPDATE databasecontroll.table_log Set status=?, date_update=? Where id="+id1+"";
      try {
    	  PreparedStatement	 ps = connection.prepareStatement(sql);
    	  ps.setString(1, status);
    	  ps.setString(2, date_staging);
    	  ps.executeUpdate();
    	  return true;
      }catch(SQLException e) {
    	 e.printStackTrace(); 
  
	return false;
}
}


	public void LoadFile1(String table_name,List<SinhVien> listBooks,String filed_name,int colnum,String stt,String id1) throws ClassNotFoundException, SQLException, IOException {
String sql = null;
PreparedStatement ps = null;
LoadFile load = new LoadFile();
		   ConnectionDB connect = new ConnectionDB();
	       Connection  connection = connect.loadProps();
	       try {
	    	   if(colnum==11) {
		 sql = "INSERT INTO database_staging." + table_name + "(" + filed_name+")"+ "   VALUES(?,?,?,?,?,?,?,?,?,?,?) " ;
		System.out.println(sql);
	 ps = connection.prepareStatement(sql);
		for (SinhVien book : listBooks) {
			 ps.setString(1,book.getStt());
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
	    	   }
	    	   if(colnum==7) {
	    		   sql = "INSERT INTO database_staging." + table_name + "(" + filed_name+")"+ "   VALUES(?,?,?,?,?,?,?) " ;
	    			System.out.println(sql);
	    		 ps = connection.prepareStatement(sql);
	    			for (SinhVien book : listBooks) {
	    				 ps.setString(1,book.getStt());
	    				 ps.setString(2, book.getMaSV());
	    				 ps.setString(3, book.getHoLot());
	    				 ps.setString(4, book.getTen());
	    				 ps.setString(5, book.getNgaySinh());
	    				 ps.setString(6, book.getMaLop());
	    				 ps.setString(7, book.getTenLop());

	    				 ps.addBatch(); 
	    	   }
	    	   }
	    	   if(colnum==5) {
	    		   sql = "INSERT INTO database_staging." + table_name + "(" + filed_name+")"+ "   VALUES(?,?,?,?,?) " ;
	    			System.out.println(sql);
	    		 ps = connection.prepareStatement(sql);
	    			for (SinhVien book : listBooks) {
	    				 ps.setString(1,book.getStt());
	    				 ps.setString(2, book.getMaSV());
	    				 ps.setString(3, book.getHoLot());
	    				 ps.setString(4, book.getTen());
	    				 ps.setString(5, book.getNgaySinh());

	    				 ps.addBatch(); 
	    	   }
	    	   }   if(colnum==4) {
	    		   sql = "INSERT INTO database_staging." + table_name + "(" + filed_name+")"+ "   VALUES(?,?,?,?) " ;
	    			System.out.println(sql);
	    		 ps = connection.prepareStatement(sql);
	    			for (SinhVien book : listBooks) {
	    				 ps.setString(1,book.getStt());
	    				 ps.setString(2, book.getMaSV());
	    				 ps.setString(3, book.getHoLot());
	    				 ps.setString(4, book.getTen());

	    				 ps.addBatch(); 
	    	   }
	    	   }
				ps.executeBatch();
				ps.close();
				String date_staging = load.getTime();
				String status = "Upload_Ok";
				updateLog(status, date_staging,id1);
				
	       }catch (Exception e) {
            
           e.printStackTrace();
         
            
       }
	
}
	public void  checkFile(String file,String table_name,List<SinhVien> listBooks,String filed_name,int colnum,String stt,String id) throws IOException, SQLException, ClassNotFoundException {

		LoadFile lf = new LoadFile();	
		

		try {
			if (stt.equals("Download_OK")) {
				lf.LoadFile1(table_name, listBooks, filed_name, colnum, stt,id);
				System.out.println("Insert thành công");
			}else {
		System.out.println("File đã được insert và không thể insert thêm ");
			}
			
		} catch (SQLException e) {

			e.printStackTrace();
		}
		
	}

	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		LoadFile lf = new LoadFile();
		ConnectionDB connect = new ConnectionDB();

		Scanner sc = new Scanner(System.in);
		System.out.println("Nhập loại config_id:");
	    String id = sc.nextLine();
	PreparedStatement ps;
	       Connection  connection = connect.loadProps();       
	    String sql = "SELECT *  from databasecontroll.table_config c, databasecontroll.table_log l  where l.config_id = c.id and l.config_id='"+id+"'";
String sql1= "SELECT l.id  from databasecontroll.table_config c, databasecontroll.table_log l  where l.config_id = c.id and l.config_id='"+id+"'";
PreparedStatement ps1 = connection.prepareStatement(sql1);	
ResultSet rs1 = ps1.executeQuery();
while (rs1.next()) {
	
String idfile = rs1.getString("id");
System.out.println("Có các loại file sau :" + idfile);}
System.out.println("Nhập id:");
String id1 = sc.nextLine();
sql+= "and l.id="+id1;
ps = connection.prepareStatement(sql);
	System.out.println(sql);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
		String  column_list = rs.getString("filed_name");
		String file1 = rs.getString("folder_local") + "\\" + rs.getString("name_file");
		System.out.println(file1);
		int col = rs.getInt("column");
		String table_name = rs.getString("table_name");
		String filed_name = rs.getString("filed_name");	
		String stt = rs.getString("status");
	String ten =rs.getString("name_file");
//	System.out.println(ten);
		   List<SinhVien> listBooks = new ReadFileExcel().readBooksFromExcelFile(file1);
//		System.out.println(lf.createTable(table_name, column_list,stt));
			System.out.println(lf.checkTableExist(table_name,column_list,stt));
//		lf.LoadFile11(table_name,file1,filed_name,col,stt);
		lf.checkFile(file1, table_name, listBooks, filed_name, col, stt,id1);
//		lf.getTime();
	}
	    }

	    }
	

	



