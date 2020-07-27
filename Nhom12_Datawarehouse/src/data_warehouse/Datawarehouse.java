package data_warehouse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Scanner;

import com.mysql.cj.jdbc.DatabaseMetaData;

import connectionDB.ConnectionDB;

public class Datawarehouse {
	ConnectionDB connect = new ConnectionDB();
	ConnectionDB1 connect1 = new ConnectionDB1();
	ConnectionDB2 connect2 = new ConnectionDB2();

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
		// *Get name source and des
		PreparedStatement ps_getTableNameFromConfig = connection
				.prepareStatement("Select table_name,table_warehouse from table_config where id='" + id + "'");
		ResultSet rs_getTableNameFromConfig = ps_getTableNameFromConfig.executeQuery();
		while (rs_getTableNameFromConfig.next()) {
			boolean exist = false;
			// *Get all name table in DW
			PreparedStatement ps_getAllTableFromDW = connection2.prepareStatement(
					"select TABLE_NAME from information_schema.TABLES where table_schema = 'database_warehouse'");
			ResultSet rs_getAllTableFromDW = ps_getAllTableFromDW.executeQuery();
			while (rs_getAllTableFromDW.next()) {
				// *If table have been exist don't create
				if (rs_getTableNameFromConfig.getString(2).equals(rs_getAllTableFromDW.getString(1))) {
					exist = true;
				}
			}
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
				sql += rs_getTableNameFromConfig.getString(2) + "(";
				for (int i = 1; i < num_of_col + 1; i++) {
					if (i == num_of_col) {
						sql += rsmd_getFromSoure.getColumnName(i).replaceAll(" ", "").replaceAll("/", "")
								+ " varchar(255))";
						break;
					}
					String col_name = rsmd_getFromSoure.getColumnName(i).replaceAll(" ", "").replaceAll("/", "");
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

	public void copyToDataWarehouse(int id) throws ClassNotFoundException, IOException, SQLException {
		Connection connection = connect.loadProps();
		Connection connection1 = connect1.loadProps();
		Connection connection2 = connect2.loadProps();

		PreparedStatement ps_getTableNameFromConfig = connection
				.prepareStatement("Select id,table_name,table_warehouse from table_config where id='" + id + "'");
		ResultSet rs_getTableNameFromConfig = ps_getTableNameFromConfig.executeQuery();
		while (rs_getTableNameFromConfig.next()) {
			// *Get status of log
			PreparedStatement ps_getStatus = connection
					.prepareStatement("Select status from table_log where id = '" + id + "'");
			ResultSet rs_getStatus = ps_getStatus.executeQuery();
			while (rs_getStatus.next()) {
				// *Check status
				if (rs_getStatus.getString(1).equals("Download_OK")) {

					// *Get all of staging
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
				} else {
					// *Send mail fail because source is not dowload_OK
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