package data_warehouse;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Scanner;
import connectionDB.ConnectionDB;

public class Datawarehouse {
	ConnectionDB connect = new ConnectionDB();
	ConnectionDB1 connect1 = new ConnectionDB1();
	ConnectionDB2 connect2 = new ConnectionDB2();
	Properties pros = new Properties();
	// *connection ket noi Control
	// *connection1 ket noi Staging
	// *connection2 ket noi Datawarehouse

	public void createDatabase(int id) throws ClassNotFoundException, IOException, SQLException {
		Connection connection = connect.loadProps();
		Connection connection1 = connect1.loadProps();
		Connection connection2 = connect2.loadProps();
		FileInputStream f = new FileInputStream("src/data_warehouse/dms_config.properties");
		pros.load(f);
		final String DATABASE_2 = pros.getProperty("database2");
		final String DATABASE_3 = pros.getProperty("database3");
		int countTableInStaging = 0;
		int count = 0;
		// *Lay ten cua table trong staging va trong datawarehouse tu
		// table_config
		PreparedStatement ps_getTableNameFromConfig = connection
				.prepareStatement("Select table_name,table_warehouse from table_config where id='" + id + "'");
		ResultSet rs_getTableNameFromConfig = ps_getTableNameFromConfig.executeQuery();
		while (rs_getTableNameFromConfig.next()) {
			boolean exist = false;
			// *Kiem tra trong staging
			// *Lay tat ca cac ten cua table trong staging
			PreparedStatement ps_getTableNameOfStaging = connection1.prepareStatement(
					"select TABLE_NAME from information_schema.TABLES where table_schema = '" + DATABASE_2 + "'");
			ResultSet rs_getTableNameOfStaging = ps_getTableNameOfStaging.executeQuery();
			while (rs_getTableNameOfStaging.next()) {
				countTableInStaging++;
				count++;
				if (rs_getTableNameOfStaging.getString(1).equals(rs_getTableNameFromConfig.getString(1))) {
					count--;
					// *Lay tat ca cac ten table trong datawarehouse
					PreparedStatement ps_getAllTableFromDW = connection2
							.prepareStatement("select TABLE_NAME from information_schema.TABLES where table_schema = '"
									+ DATABASE_3 + "'");
					ResultSet rs_getAllTableFromDW = ps_getAllTableFromDW.executeQuery();
					while (rs_getAllTableFromDW.next()) {
						// *Kiem tra table co ton tai hay chua
						if (rs_getTableNameFromConfig.getString(2).equals(rs_getAllTableFromDW.getString(1))) {
							// *Already exist
							exist = true;
						}
					}
					// *Tao table
					if (!exist) {
						// *Lay tat ca cua table staging
						PreparedStatement ps_getFromSource = connection1
								.prepareStatement("Select * from " + rs_getTableNameFromConfig.getString(1));
						ResultSet rs_getFromSource = ps_getFromSource.executeQuery();
						ResultSetMetaData rsmd_getFromSoure = rs_getFromSource.getMetaData();
						// *Lay tong so cot cua table staging
						int num_of_col = rsmd_getFromSoure.getColumnCount();
						// *Tao cau sql
						String sql = "Create table ";
						sql += rs_getTableNameFromConfig.getString(2).replaceAll(" ", "") + "(";
						for (int i = 1; i < num_of_col + 1; i++) {
							if (i == num_of_col) {
								sql += rsmd_getFromSoure.getColumnName(i).replaceAll(" ", "").replaceAll("/", "")
										+ " varchar(255))";
								break;
							}
							String col_name = rsmd_getFromSoure.getColumnName(i).replaceAll(" ", "").replaceAll("/",
									"");
							sql += col_name + " varchar(255),";
						}
						PreparedStatement ps_createDB = connection2.prepareStatement(sql);
						ps_createDB.executeUpdate();
						System.out.println("Tao table thanh cong!");
					} else {
						System.out.println("Table da ton tai!");
					}
				}
			}
		}
		if (count == countTableInStaging) {
			System.out.println("Trong staging khong co table nay");
		}

	}

	public void copyToDataWarehouse(int id) throws ClassNotFoundException, IOException, SQLException {
		Connection connection = connect.loadProps();
		Connection connection1 = connect1.loadProps();
		Connection connection2 = connect2.loadProps();
		FileInputStream f = new FileInputStream("src/data_warehouse/dms_config.properties");
		pros.load(f);
		final String DATABASE_2 = pros.getProperty("database2");

		PreparedStatement ps_getTableNameFromConfig = connection
				.prepareStatement("Select id,table_name,table_warehouse from table_config where id='" + id + "'");
		ResultSet rs_getTableNameFromConfig = ps_getTableNameFromConfig.executeQuery();
		while (rs_getTableNameFromConfig.next()) {
			// *Kiem tra co ton tai trong staging
			PreparedStatement ps_getTableNameOfStaging = connection1.prepareStatement(
					"select TABLE_NAME from information_schema.TABLES where table_schema = '" + DATABASE_2 + "'");
			ResultSet rs_getTableNameOfStaging = ps_getTableNameOfStaging.executeQuery();
			while (rs_getTableNameOfStaging.next()) {
				if (rs_getTableNameOfStaging.getString(1).equals(rs_getTableNameFromConfig.getString(2))) {
					// *Lay tat ca trong staging
					PreparedStatement ps_getFromStaging = connection1
							.prepareStatement("Select * from " + rs_getTableNameFromConfig.getString(2));
					ResultSet rs_getFromStaging = ps_getFromStaging.executeQuery();
					ResultSetMetaData rsmd_getFromStaging = rs_getFromStaging.getMetaData();
					// *Dem so cot cua table trong staging
					int colOfStaging = rsmd_getFromStaging.getColumnCount();
					// *Lay tat ca trong datawarehouse
					PreparedStatement ps_getFromDW = connection2
							.prepareStatement("Select * from " + rs_getTableNameFromConfig.getString(3));
					ResultSet rs_getFromDW = ps_getFromDW.executeQuery();
					ResultSetMetaData rsmd_getFromDW = rs_getFromDW.getMetaData();
					// *Dem so cot cua table trong datawarehouse
					int colOfDW = rsmd_getFromDW.getColumnCount();
					// *Kiem tra 2 cot
					if (colOfDW == colOfStaging) {
						// *Neu 2 cot bang nhau
						String sql = "Insert into " + rs_getTableNameFromConfig.getString(3) + " values(";
						for (int i = 1; i < colOfDW + 1; i++) {
							if (i == colOfDW) {
								sql += "?)";
								break;
							}
							sql += "?,";
						}
						while (rs_getFromStaging.next()) {
							// *Them du lieu vao datawarehouse
							PreparedStatement ps_insertToDB = connection2.prepareStatement(sql);
							for (int i = 1; i < colOfDW + 1; i++) {
								ps_insertToDB.setString(i, rs_getFromStaging.getString(i));
							}
							ps_insertToDB.executeUpdate();
							// *Xoa
							PreparedStatement ps_delete = connection1
									.prepareStatement("Delete from " + rs_getTableNameFromConfig.getString(2)
											+ " where " + rsmd_getFromStaging.getColumnName(1) + " ='"
											+ rs_getFromStaging.getString(1) + "'");
							ps_delete.executeUpdate();
							System.out.println("Du lieu da duoc copy!");
						}
					} else {
						System.out.println("Khong copy");
					}
				}
			}
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException {
		Datawarehouse d = new Datawarehouse();
		while (true) {
			System.out.println("Nhap id:");
			Scanner sc = new Scanner(System.in);
			int id = sc.nextInt();
			d.createDatabase(id);
			d.copyToDataWarehouse(id);
		}
	}
}