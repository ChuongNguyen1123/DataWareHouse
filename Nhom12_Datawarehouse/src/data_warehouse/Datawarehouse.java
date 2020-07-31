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
	// *connection connect to Control
	// *connection1 connect to Staging
	// *connection2 connect to Datawarehouse

	public void createDatabase(int id) throws ClassNotFoundException, IOException, SQLException {
		// *Connect to Control
		Connection connection = connect.loadProps();
		// *Connect to Staging
		Connection connection1 = connect1.loadProps();
		// *Connect to Datawarehouse
		Connection connection2 = connect2.loadProps();
		FileInputStream f = new FileInputStream("src/data_warehouse/dms_config.properties");
		pros.load(f);
		final String DATABASE_1 = pros.getProperty("database1");
		final String DATABASE_2 = pros.getProperty("database2");
		final String DATABASE_3 = pros.getProperty("database3");
		int countTableInStaging = 0;
		int count = 0;
		// *Get name in staging and datawarehouse
		PreparedStatement ps_getTableNameFromConfig = connection
				.prepareStatement("Select table_name,table_warehouse from table_config where id='" + id + "'");
		ResultSet rs_getTableNameFromConfig = ps_getTableNameFromConfig.executeQuery();
		while (rs_getTableNameFromConfig.next()) {
			boolean exist = false;
			// *Check in staging
			// Get all name table in staging
			PreparedStatement ps_getTableNameOfStaging = connection1.prepareStatement(
					"select TABLE_NAME from information_schema.TABLES where table_schema = '" + DATABASE_2 + "'");
			ResultSet rs_getTableNameOfStaging = ps_getTableNameOfStaging.executeQuery();
			while (rs_getTableNameOfStaging.next()) {
				countTableInStaging++;
				count++;
				if (rs_getTableNameOfStaging.getString(1).equals(rs_getTableNameFromConfig.getString(1))) {
					count--;
					// *Get all name table in DW
					PreparedStatement ps_getAllTableFromDW = connection2
							.prepareStatement("select TABLE_NAME from information_schema.TABLES where table_schema = '"
									+ DATABASE_3 + "'");
					ResultSet rs_getAllTableFromDW = ps_getAllTableFromDW.executeQuery();
					while (rs_getAllTableFromDW.next()) {
						// *If the table exists then do not create
						if (rs_getTableNameFromConfig.getString(2).replaceAll(" ", "")
								.equals(rs_getAllTableFromDW.getString(1))) {
							// *Already exist
							exist = true;
						}
					}
					// *Create table
					if (!exist) {
						// *Get all in source
						PreparedStatement ps_getFromSource = connection1
								.prepareStatement("Select * from " + rs_getTableNameFromConfig.getString(1));
						ResultSet rs_getFromSource = ps_getFromSource.executeQuery();
						ResultSetMetaData rsmd_getFromSoure = rs_getFromSource.getMetaData();
						// *Get num of col in table source
						int num_of_col = rsmd_getFromSoure.getColumnCount();
						// *Create sql to create database
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
		// *Connect to Control
		Connection connection = connect.loadProps();
		// *Connect to Staging
		Connection connection1 = connect1.loadProps();
		// *Connect to Datawarehouse
		Connection connection2 = connect2.loadProps();
		FileInputStream f = new FileInputStream("src/data_warehouse/dms_config.properties");
		pros.load(f);
		final String DATABASE_2 = pros.getProperty("database2");

		PreparedStatement ps_getTableNameFromConfig = connection
				.prepareStatement("Select id,table_name,table_warehouse from table_config where id='" + id + "'");
		ResultSet rs_getTableNameFromConfig = ps_getTableNameFromConfig.executeQuery();
		while (rs_getTableNameFromConfig.next()) {
			// *Check staging
			PreparedStatement ps_getTableNameOfStaging = connection1.prepareStatement(
					"select TABLE_NAME from information_schema.TABLES where table_schema = '" + DATABASE_2 + "'");
			ResultSet rs_getTableNameOfStaging = ps_getTableNameOfStaging.executeQuery();
			while (rs_getTableNameOfStaging.next()) {
				if (rs_getTableNameOfStaging.getString(1).equals(rs_getTableNameFromConfig.getString(2))) {
					// *Get all of table staging
					PreparedStatement ps_getFromStaging = connection1
							.prepareStatement("Select * from " + rs_getTableNameFromConfig.getString(2));
					ResultSet rs_getFromStaging = ps_getFromStaging.executeQuery();
					ResultSetMetaData rsmd_getFromStaging = rs_getFromStaging.getMetaData();
					// *Count col of staging
					int colOfStaging = rsmd_getFromStaging.getColumnCount();

					// *Get all of DW
					PreparedStatement ps_getFromDW = connection2
							.prepareStatement("Select * from " + rs_getTableNameFromConfig.getString(3));

					ResultSet rs_getFromDW = ps_getFromDW.executeQuery();
					ResultSetMetaData rsmd_getFromDW = rs_getFromDW.getMetaData();
					// *Count col of dw
					int colOfDW = rsmd_getFromDW.getColumnCount();
					// *Check col of 2 database
					if (colOfDW == colOfStaging) {
						// * Create sql to copy
						String sql = "Insert into " + rs_getTableNameFromConfig.getString(3) + " values(";
						for (int i = 1; i < colOfDW + 1; i++) {
							if (i == colOfDW) {
								sql += "?)";
								break;
							}
							sql += "?,";
						}
						while (rs_getFromStaging.next()) {
							// Insert into dw
							PreparedStatement ps_insertToDB = connection2.prepareStatement(sql);
							for (int i = 1; i < colOfDW + 1; i++) {
								ps_insertToDB.setString(i, rs_getFromStaging.getString(i));
							}
							ps_insertToDB.executeUpdate();
							// *Delete
							PreparedStatement ps_delete = connection1
									.prepareStatement("Delete from " + rs_getTableNameFromConfig.getString(2)
											+ " where " + rsmd_getFromStaging.getColumnName(1) + " ='"
											+ rs_getFromStaging.getString(1) + "'");
							ps_delete.executeUpdate();
							System.out.println("Du lieu da duoc copy!");
							// send mail success
						}
					} else {
						// *Send mail fail because not same number of column
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