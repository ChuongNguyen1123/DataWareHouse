package loadStaging;

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
import data_warehouse.CopyToWarehouse;
import download.SendMailSSL;

public class LoadFile {
	// Kiểm tra File
	public void checkFile(String file, String table_name, List<SinhVien> listBooks, String filed_name, int colnum,
			String stt, String id, String nameFile) throws IOException, SQLException, ClassNotFoundException {
		SendMailSSL sendmail = new SendMailSSL();
//		LoadFile loadfile = new LoadFile();
		try {
			// Bước 4.Kiểm tra đã LoadFile1() vào database database_staging
			// Nếu file đã được load thì không load thêm vào nữa
			
			if (stt.equals("Upload_Ok")) {
				System.out.println("File " + nameFile + " đã được insert");	
			}
			else
			System.out.println("Bat dau Insert flie " + nameFile);
				// Bước 5.Lấy ra các file có stt = "Download_Ok"
				// Nếu status là "Download_Fail" thì loadfile không thành không
				if (stt.equals("Download_Fail")) {
				System.out.println("Insert file " + nameFile + " không thành công");
				String Tieude = "Data Warehouse nhom 12 - Ca sang";
				String noiDung = "Insert file " + nameFile + " không thành công";
				// Bước 6.Gửi mail thông báo Insert không thành công
				sendmail.sendMail(Tieude, noiDung);
				// Bước 7.Cập nhập status = Upload_Fail và thời gian
				String date_staging = getTime();
				String status = "Upload_Fail";
				updateLog(status, date_staging, id);
			}
			else
	
			// Nếu status là "Download_OK" thì tiến hành loadfile
			if (stt.equals("Download_OK")) {
				// Bước 8.LoadFile1() vào database database_staging
				LoadFile1(table_name, listBooks, filed_name, colnum, stt, id);
				System.out.println("Insert file " + nameFile + " thành công");
				String date_staging = getTime();
				String status = "Upload_Ok";
				// Bước 9.Cập nhập status = Upload_OK và thời gian
				// Cập nhập Log
				updateLog(status, date_staging, id);

			}
		} catch (SQLException e) {

			e.printStackTrace();
		}

	}
//Load File vào database
	public void LoadFile1(String table_name, List<SinhVien> listSinhVien, String filed_name, int column, String stt,
			String id1) throws ClassNotFoundException, SQLException, IOException {
		String sql = null;
		PreparedStatement ps = null;
		LoadFile loadfile = new LoadFile();
		// Kết nối database
		ConnectionDBStaging connect = new ConnectionDBStaging();
		Connection connection = connect.loadProps();
		try {
			if (column == 11) {
				sql = "INSERT INTO " + table_name + "(" + filed_name + ")" + "   VALUES(?,?,?,?,?,?,?,?,?,?,?) ";
//				System.out.println(sql);
				// Gắn các giá trị vào trong tham số
				ps = connection.prepareStatement(sql);
				// Duyệt danh sách sinh viên
				for (SinhVien sinhvien : listSinhVien) {
					ps.setString(1, sinhvien.getStt());
					ps.setString(2, sinhvien.getMaSV());
					ps.setString(3, sinhvien.getHoLot());
					ps.setString(4, sinhvien.getTen());
					ps.setString(5, sinhvien.getNgaySinh());
					ps.setString(6, sinhvien.getMaLop());
					ps.setString(7, sinhvien.getTenLop());
					ps.setString(8, sinhvien.getDtLienLac());
					ps.setString(9, sinhvien.getEmail());
					ps.setString(10, sinhvien.getQueQuan());
					ps.setString(11, sinhvien.getGhiChu());
					ps.addBatch();

				}
			}
			if (column == 7) {
				sql = "INSERT INTO " + table_name + "(" + filed_name + ")" + "   VALUES(?,?,?,?,?,?,?) ";
//				System.out.println(sql);
				ps = connection.prepareStatement(sql);
				for (SinhVien sinhvien : listSinhVien) {
					ps.setString(1, sinhvien.getStt());
					ps.setString(2, sinhvien.getMaSV());
					ps.setString(3, sinhvien.getHoLot());
					ps.setString(4, sinhvien.getTen());
					ps.setString(5, sinhvien.getNgaySinh());
					ps.setString(6, sinhvien.getMaLop());
					ps.setString(7, sinhvien.getTenLop());

					ps.addBatch();
				}
			}
			if (column == 5) {
				sql = "INSERT INTO " + table_name + "(" + filed_name + ")" + "   VALUES(?,?,?,?,?) ";
//				System.out.println(sql);
				ps = connection.prepareStatement(sql);
				for (SinhVien sinhvien : listSinhVien) {
					ps.setString(1, sinhvien.getStt());
					ps.setString(2, sinhvien.getMaSV());
					ps.setString(3, sinhvien.getHoLot());
					ps.setString(4, sinhvien.getTen());
					ps.setString(5, sinhvien.getNgaySinh());

					ps.addBatch();
				}
			}
			if (column == 4) {
				sql = "INSERT INTO " + table_name + "(" + filed_name + ")" + "   VALUES(?,?,?,?) ";
//				System.out.println(sql);
				ps = connection.prepareStatement(sql);
				for (SinhVien sinhvien : listSinhVien) {
					ps.setString(1, sinhvien.getStt());
					ps.setString(2, sinhvien.getMaSV());
					ps.setString(3, sinhvien.getHoLot());
					ps.setString(4, sinhvien.getTen());

					ps.addBatch();
				}
			}
			ps.executeBatch();
			ps.close();

		} catch (Exception e) {
			// Xử lý lỗi cho JDBC
			e.printStackTrace();

		}

	}

	// Lấy ngày tháng năm giờ hiện tại
	public static String getTime() {
		// Kiểu định dang này nếu ngày tháng năm giờ chỉ có 1 số thì sẽ tự động thêm số
		// 0 đằng trước
		DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		// Lấy thời gian hiện tại
		Date date = new Date();
		System.out.println(df.format(date));
		return df.format(date);

	}

	// Cập nhập log
	public static boolean updateLog(String status, String date_staging, String id) throws IOException, SQLException {
		// Kết nối database
		ConnectionDB connect = new ConnectionDB();
		Connection connection = connect.loadProps();
		String sql = "UPDATE table_log Set status=?, date_update=? Where id=" + id + "";
		try {
			// Gắn các giá trị vào tham số
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, status);
			ps.setString(2, date_staging);
			// Cạp nhập log
			ps.executeUpdate();
			return true;
		} catch (SQLException e) {
			// Xử lý lỗi cho JDBC
			e.printStackTrace();

			return false;
		}
	}

	public void run() throws ClassNotFoundException, SQLException, IOException {
		// Kết nối database
		LoadFile loadfile = new LoadFile();
		ConnectionDB connect = new ConnectionDB();
		Connection connection = connect.loadProps();
		
		CopyToWarehouse copy = new CopyToWarehouse();
		
		// in ra danh sách trong bảng log và config
		
		String sqlSelectLog = "SELECT *  from table_config c, table_log l  where l.config_id = c.id";
		PreparedStatement prepaSelectLog = connection.prepareStatement(sqlSelectLog);
		ResultSet rs = prepaSelectLog.executeQuery();
		while (rs.next()) {
			int idLog = rs.getInt("l.id");
			String nameFileLog = rs.getString("name_file");
			String file1 = rs.getString("folder_local") + "\\" + nameFileLog;
			int col = rs.getInt("column");
			String table_name = rs.getString("table_name");
			String filed_name = rs.getString("filed_name");
			String stt = rs.getString("status");
			String ten = rs.getString("name_file");
			List<SinhVien> listSinhVien = new ReadFileExcel().readFileFromExcelFile(file1);
			loadfile.checkFile(file1, table_name, listSinhVien, filed_name, col, stt, String.valueOf(idLog), nameFileLog);
			copy.copy(idLog);
		}
	}
}
