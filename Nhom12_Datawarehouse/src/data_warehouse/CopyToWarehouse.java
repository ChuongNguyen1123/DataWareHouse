package data_warehouse;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.Scanner;

import connectionDB.ConnectionDB;

public class CopyToWarehouse {
	ConnectionDB connect = new ConnectionDB();
	ConnectionDB1 connect1 = new ConnectionDB1();
	ConnectionDB2 connect2 = new ConnectionDB2();
	Properties pros = new Properties();

	public void copy(int id) throws IOException, SQLException, ClassNotFoundException {
		Connection connection = connect.loadProps();
		Connection connection1 = connect1.loadProps();
		Connection connection2 = connect2.loadProps();
		FileInputStream f = new FileInputStream("src/data_warehouse/dms_config.properties");
		pros.load(f);
		final String DATABASE_2 = pros.getProperty("database2");
		final String DATABASE_3 = pros.getProperty("database3");
		PreparedStatement ps_getFromLog = connection.prepareStatement("SELECT config_id from table_log where id=" +id);
		ResultSet rs_getFromLog = ps_getFromLog.executeQuery();
		while(rs_getFromLog.next()) {
		// Lay du lieu tu config
		PreparedStatement ps_getDataFromConfig = connection
				.prepareStatement("SELECT table_name,table_warehouse FROM table_config where id = " + rs_getFromLog.getInt(1));
		ResultSet rs_getDataFromConfig = ps_getDataFromConfig.executeQuery();
		while (rs_getDataFromConfig.next()) {
			// Kiem tra ton tai table trong staging va DW
			boolean existInStaging = false;// bien kiem tra trong staging
			boolean existInDW = false;// bien kiem tra trong DW
			// Lay het cac table trong staging
			PreparedStatement ps_checkTableInStaging = connection1.prepareStatement(
					"SELECT TABLE_NAME FROM information_schema.TABLES WHERE table_schema = '" + DATABASE_2 + "'");
			ResultSet rs_checkTableInStaging = ps_checkTableInStaging.executeQuery();
			while (rs_checkTableInStaging.next()) {
				// Kiem tra ten table trong staging lay trong config co ton tai trong database staging
				if (rs_getDataFromConfig.getString(1).equals(rs_checkTableInStaging.getString(1))) {
					existInStaging = true;
					// Lay het cac table trong staging
					PreparedStatement ps_checkTableInDW = connection2
							.prepareStatement("SELECT TABLE_NAME FROM information_schema.TABLES WHERE table_schema = '"
									+ DATABASE_3 + "'");
					ResultSet rs_checkTableInDW = ps_checkTableInDW.executeQuery();
					while (rs_checkTableInDW.next()) {
						// Kiem tra ten table trong DW lay trong config co ton tai trong database DW
						if (rs_getDataFromConfig.getString(2).equals(rs_checkTableInDW.getString(1))) {
							existInDW = true;
							break;
						}
					}
					break;
				}
			}
			// Neu ca 2 table deu ton tai
			if (existInDW && existInStaging) {
				// Lay het cac du lieu trong table staging lay tu config
				PreparedStatement ps_getAllFromStaging = connection1
						.prepareStatement("SELECT * FROM " + rs_getDataFromConfig.getString(1));
				ResultSet rs_getAllFromStaging = ps_getAllFromStaging.executeQuery();
				// Lay ra so cot cua table trong staging
				ResultSetMetaData rsmd_getAllFromStaging = rs_getAllFromStaging.getMetaData();
				int countColOfStaging = rsmd_getAllFromStaging.getColumnCount();

				// Lay het cac du lieu trong table DW lay tu config
				PreparedStatement ps_getAllFromDW = connection2
						.prepareStatement("SELECT * FROM " + rs_getDataFromConfig.getString(2));
				ResultSet rs_getAllFromDW = ps_getAllFromDW.executeQuery();
				// Lay ra so cot cua table trong DW
				ResultSetMetaData rsmd_getAllFromDW = rs_getAllFromDW.getMetaData();
				int countColOfDW = rsmd_getAllFromDW.getColumnCount();
				// Neu 2 cot cung so cot thi tinh tiep
				if (countColOfDW == countColOfStaging) {
					// Kiem tra ten cot
					int count = 0;// Bien kiem tra trung ten cot
					for (int i = 1; i < countColOfDW + 1; i++) {
						if (rsmd_getAllFromDW.getColumnName(i).equals(rsmd_getAllFromStaging.getColumnName(i))) {
							count++;
						}
					}
					// Neu nhu tat ca cac cot trung nhau
					if (count == countColOfDW) {
						// Tao cau lenh sql de copy
						String sql = "INSERT INTO " + rs_getDataFromConfig.getString(2) + " VALUES(";
						for (int i = 0; i < count; i++) {
							if (i == count - 1) {
								sql += "?)";
								break;
							}
							sql += "?,";
						}
						// Copy
						// Lay du lieu tu staging
						while (rs_getAllFromStaging.next()) {
							// Thuc hien insert vao DW
							PreparedStatement ps_copyToDW = connection2.prepareStatement(sql);
							for (int i = 1; i < count + 1; i++) {
								// Neu cot co kieu du lieu la int
								if (rsmd_getAllFromDW.getColumnTypeName(i).contains("INT")) {
									// Neu du lieu trong
									if (rs_getAllFromStaging.getString(i) == null
											|| rs_getAllFromStaging.getString(i).equals("")
											|| rs_getAllFromStaging.getString(i).equals(" ")) {
										// Cho mac dinh la 0
										ps_copyToDW.setInt(i, 0);
									} else {
										ps_copyToDW.setInt(i, convertToInt(rs_getAllFromStaging.getString(i)));
									}
								}
								// Neu cot co kieu du lieu la varchar
								if (rsmd_getAllFromDW.getColumnTypeName(i).contains("VARCHAR")) {
									// Neu du lieu trong
									if (rs_getAllFromStaging.getString(i) == null
											|| rs_getAllFromStaging.getString(i).equals("")
											|| rs_getAllFromStaging.getString(i).equals(" ")) {
										// Cho mac dinh la Null
										ps_copyToDW.setString(i, "Null");
									} else {
										ps_copyToDW.setString(i, rs_getAllFromStaging.getString(i));
									}
								}
								// Neu cot co kieu du lieu la date
								if (rsmd_getAllFromDW.getColumnTypeName(i).contains("DATE")) {
									// Neu du lieu trong
									if (rs_getAllFromStaging.getString(i) == null
											|| rs_getAllFromStaging.getString(i).equals("")
											|| rs_getAllFromStaging.getString(i).equals(" ")) {
										// Cho mac dinh la ngay 1970-1-1
										ps_copyToDW.setDate(i, convertToDate("1970-1-1"));
									} else {
										// Chuyen ve kieu date trong sql dung dang Y-M-D
										ps_copyToDW.setDate(i,
												convertToDate(convertToYMD(rs_getAllFromStaging.getString(i))));
									}
								}
							}
							ps_copyToDW.executeUpdate();
							// Xoa trong staging
							PreparedStatement ps_deleteInStaging = connection1
									.prepareStatement("Delete from " + rs_getDataFromConfig.getString(1) + " where "
											+ rsmd_getAllFromStaging.getColumnName(1) + " ='"
											+ rs_getAllFromStaging.getInt(1) + "'");
							ps_deleteInStaging.executeUpdate();
						}
						// Update thoi gian copy len log
						PreparedStatement ps_setTimeUpdate = connection.prepareStatement(
								"UPDATE table_log set date_warehouse  ='" + getTime() + "' " + "where id=" + id);
						ps_setTimeUpdate.executeUpdate();
						System.out.println("Success");
					} else {
						System.out.println("Khong copy vi khong trung ten cot");
					}
				} else {
					System.out.println("Khong copy vi khong cung so luong cot");
				}
			} else {
				System.out.println("Table khong ton tai");
			}
		}
	}
	}
	public int convertToInt(String type) {
		return Integer.parseInt(type);
	}

	// Chuyen ve Y-M-D
	public String convertToYMD(String time) {
		String first = "";
		// Neu la kieu String co dau phan cach la /
		// Vi du 20/12/2000, 2000/12/20
		if (time.contains("/")) {
			String[] list = time.split("/");
			for (String s : list) {
				first += s;
				break;
			}
			if (Integer.parseInt(first) > 31) {
				time.replaceAll("/", "-");
				return time;
			} else {
				String day = "";
				String month = "";
				String year = "";
				for (String s : list) {
					if (day.equals("")) {
						day += s;
					} else if (month.equals("") && !day.equals("")) {
						month += s;
					} else if (year.equals("") && !day.equals("") && !month.equals("")) {
						year += s;
						return year + "-" + month + "-" + day;
					}
				}
			}

			// Neu la kieu String co dau phan cach la -
			// Vi du 12-9-2000, 2000-9-12
		}
		if (time.contains("-")) {
			String[] list = time.split("-");
			for (String s : list) {
				first += s;
				break;
			}
			if (Integer.parseInt(first) > 31) {
				return time;
			} else {
				String day = "";
				String month = "";
				String year = "";
				for (String s : list) {
					if (day.equals("")) {
						day += s;
					} else if (month.equals("") && !day.equals("")) {
						month += s;
					} else if (year.equals("") && !day.equals("") && !month.equals("")) {
						year += s;
						return year + "-" + month + "-" + day;
					}
				}
			}

		}
		return time;
	}

	// Chuyen ve time trong sql
	public Date convertToDate(String time) {
		Date date = Date.valueOf(time);
		return date;
	}

	// Lay thoi gian hien thoi
	public Timestamp getTime() {
		java.util.Date now = new java.util.Date();
		Timestamp timestamp = new Timestamp(now.getTime());
		return timestamp;
	}

}
