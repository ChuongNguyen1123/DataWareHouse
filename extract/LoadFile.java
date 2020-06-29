package extract;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;



import Check.Check;
import connection_utils.DBConnect;
import control_tool.ConnectDBControl;
import log.Log;
import log.LogSTT;
import tool.ExchangeJavaPath;

public class LoadFile {
	
	public static void createTableStaging() {

	}

	public static void loadFileToStaging() throws ClassNotFoundException, SQLException, IOException {
		
		// Lấy thông tin file data đã được tải về
		String[] infoConfig = ConnectDBControl.getConfigInformation();
		int idConfig = Integer.parseInt(infoConfig[0]);
		String fileLocalPath = infoConfig[12];
		String dataFileType = infoConfig[13];
		String dataFileName = infoConfig[14];
		String dataFileDelimiter = infoConfig[15];
		int numRecords = Integer.parseInt(infoConfig[16]);
		
		String fileLocalJavaPath = ExchangeJavaPath.exchangeToJavaPath(fileLocalPath);
		String fullPath = fileLocalPath + "/" + dataFileName + dataFileType;
		
		Connection connection = DBConnect.getConnection();
		PreparedStatement ps;
		// load file data với các định dạng khác nhau sử dụng switch case
		switch (dataFileType) {
		case ".txt":
		case ".csv":
			String sql = "LOAD DATA INFILE '" + fileLocalPath + "/" + dataFileName + dataFileType + "' "
					+ "INTO TABLE tb_staging " 
					+ "CHARACTER SET 'utf8' " 
					+ "FIELDS TERMINATED BY '" + dataFileDelimiter + "' "
					+ "ENCLOSED BY '\"' "
					+ "LINES TERMINATED BY '\\r\\n' " 
					+ "IGNORE 1 LINES "
					+ "(stt,ma_sv,ho_lot,ten,ngay_sinh,ma_lop,ten_lop,dt_lienlac,email,que_quan,ghi_chu);";

			ps = connection.prepareStatement(sql);
			ps.executeUpdate();
			System.out.println("Load data from " + fullPath + " to database STAGING");
			connection.close();
			break;
			
		case ".xxxx":
			ArrayList<String> listData = ReadFileCSV.loadDataCSV(fileLocalJavaPath, dataFileType, dataFileName);
			ArrayList<String> listDataIgnore1 = new ArrayList<String>();
			for (int i = 1; i < listData.size(); i++) {
				listDataIgnore1.add(listData.get(i));
			}
			String sql2 = "INSERT INTO tb_staging VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql2);
			for (int i = 0; i < listDataIgnore1.size(); i++) {
				String[] line = listDataIgnore1.get(i).split(",");
				ps.setInt(1, Integer.parseInt(line[0]));
				ps.setString(2, line[1]);
				ps.setString(3, line[2]);
				ps.setString(4, line[3]);
				ps.setString(5, line[4]);
				ps.setString(6, line[5]);
				ps.setString(7, line[6]);
				ps.setString(8, line[7]);
				ps.setString(9, line[8]);
				ps.setString(10, line[9]);
				ps.setString(11, line[10]);
				ps.executeUpdate();
			}
			System.out.println("Load data from " + fullPath + " to database STAGING");
			connection.close();
			break;
		case ".xlsx":
		case ".xls":
			String xlsxFile = fileLocalJavaPath + "\\" + dataFileName + dataFileType;
			List<SinhVien> dsSinhVien = ReadFileExcel.readExcel(xlsxFile);
			String sql3 = "INSERT INTO tb_staging VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			ps = connection.prepareStatement(sql3);
			for (SinhVien sv : dsSinhVien) {
				ps.setInt(1, sv.getStt());
				ps.setString(2, Integer.toString(sv.getMaSV()));
				ps.setString(3, sv.getHoLot());
				ps.setString(4, sv.getTen());
				ps.setString(5, sv.getNgaySinh());
				ps.setString(6, sv.getMaLop());
				ps.setString(7, sv.getTenLop());
				ps.setString(8, sv.getDtLienLac());
				ps.setString(9, sv.getEmail());
				ps.setString(10, sv.getQueQuan());
				ps.setString(11, sv.getGhiChu());
				ps.executeUpdate();
			}
			System.out.println("Load data from " + fullPath + " to database STAGING");
			connection.close();
			break;
		default:
			System.out.println("Không thể đọc loại file với định dạng này: " + dataFileType);
			break;
		}
		
//		Update log khi load data vào staging thành công hoặc thất bại
		String loadStatus = "";
		if (Check.checkSum("tb_staging", numRecords) == true)
		{
			loadStatus = LogSTT.ES;
		}
		else {
			loadStatus = LogSTT.EF;
		}
		
		Log.createLog(idConfig, dataFileName, dataFileType, loadStatus);
	}
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		LoadFile.loadFileToStaging();

//		String fileLocalPath = "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads";
//		String dataFileType = ".txt";
//		String dataFileName = "sinhvien_chieu_nhom4";
//		ArrayList<String> listData = ReadFromCSV.loadDataCSV(fileLocalPath, dataFileType, dataFileName);
//		Connection conn2 = MySQLConnectionUtils.getConnection();
//		ArrayList<String> listDataIgnore1 = new ArrayList<String>();
//		for (int i = 1; i < listData.size(); i++) {
//			listDataIgnore1.add(listData.get(i));
//		}
//		for (String t : listDataIgnore1) {
//			System.out.println(t);
//		}
//		conn2.close();
//		
//		for (int i = 0; i < listDataIgnore1.size(); i++) {
//			String[] line = listDataIgnore1.get(i).split(",");
//			System.out.println(line[0]);
//		}
	}
}

