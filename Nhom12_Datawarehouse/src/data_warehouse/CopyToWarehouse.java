package data_warehouse;

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

	public void copy(int id) throws IOException, SQLException, ClassNotFoundException {
		Connection connection = connect.loadProps();
		Connection connection1 = connect1.loadProps();
		Connection connection2 = connect2.loadProps();
		// Lay du lieu tu config
		PreparedStatement ps_getDataFromConfig = connection
				.prepareStatement("SELECT table_name,table_warehouse FROM table_config where id = " + id);
		ResultSet rs_getDataFromConfig = ps_getDataFromConfig.executeQuery();
		while (rs_getDataFromConfig.next()) {
			// Lay het cac du lieu trong staging
			PreparedStatement ps_getAllFromStaging = connection1
					.prepareStatement("SELECT * FROM " + rs_getDataFromConfig.getString(1));
			ResultSet rs_getAllFromStaging = ps_getAllFromStaging.executeQuery();
			// Lay ra so cot cua table trong staging
			ResultSetMetaData rsmd_getAllFromStaging = rs_getAllFromStaging.getMetaData();
			int countColOfStaging = rsmd_getAllFromStaging.getColumnCount();

			// Lay het cac du lieu trong DW
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
						PreparedStatement ps_copyToDW = connection2.prepareStatement(sql);
						for (int i = 1; i < count + 1; i++) {
							// Neu cot co kieu du lieu la int
							if (rsmd_getAllFromDW.getColumnTypeName(i).contains("INT")) {
								// Neu du lieu trong
								if (rs_getAllFromStaging.getString(i) == null
										|| rs_getAllFromStaging.getString(i).equals("")
										|| rs_getAllFromStaging.getString(i).equals(" ")) {
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
									ps_copyToDW.setString(i, "Null");
								} else {
									ps_copyToDW.setString(i, rs_getAllFromStaging.getString(i));
								}
							}
							// Neu cot co kieu du lieu la date
							if (rsmd_getAllFromDW.getColumnTypeName(i).contains("DATE")) {
								// Neu du lieu trong
//								System.out.println(convertToYMD(rs_getAllFromStaging.getString(i)));
								if (rs_getAllFromStaging.getString(i) == null
										|| rs_getAllFromStaging.getString(i).equals("")
										|| rs_getAllFromStaging.getString(i).equals(" ")) {
									ps_copyToDW.setDate(i, convertToDate("1970-1-1"));
								} else {
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
					// Update copy thanh cong len isSuccess
					PreparedStatement ps_setSuccess = connection
							.prepareStatement("UPDATE table_config set is_Success ='Success' where id=" + id);
					ps_setSuccess.executeUpdate();
					// Update thoi gian copy
					PreparedStatement ps_setTimeUpdate = connection.prepareStatement(
							"UPDATE table_config set timeCopy  ='" + getTime() + "' " + "where id=" + id);
					ps_setTimeUpdate.executeUpdate();
					System.out.println("Success");
				} else {
					System.out.println("Khong copy vi khong trung ten cot");
				}
			} else {
				System.out.println("Khong copy vi khong cung so luong cot");
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
			if (Integer.parseInt(first) > 1900) {
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
			if (Integer.parseInt(first) > 1900) {
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

	public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException {
		CopyToWarehouse d = new CopyToWarehouse();
		while (true) {
			System.out.println("Nhap id:");
			Scanner sc = new Scanner(System.in);
			int id = sc.nextInt();
			d.copy(id);
		}
	}
}
