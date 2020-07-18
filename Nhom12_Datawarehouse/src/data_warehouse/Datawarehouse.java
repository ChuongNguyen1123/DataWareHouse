package data_warehouse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.mysql.cj.jdbc.DatabaseMetaData;

import connectionDB.ConnectionDB;

public class Datawarehouse {
	ConnectionDB connect = new ConnectionDB();
	ConnectionDB1 connect1 = new ConnectionDB1();
	ConnectionDB2 connect2 = new ConnectionDB2();

	// *connection connect to Control
	// *connection1 connect to Staging
	// *connection2 connect to Datawarehouse

	public void createDatabase() throws ClassNotFoundException, IOException, SQLException {
		// *Connect to Control
		Connection connection = connect.loadProps();
		// *Connect to Staging
		Connection connection1 = connect1.loadProps();
		// *Connect to Datawarehouse
		Connection connection2 = connect2.loadProps();
		// *Get name source and des
		PreparedStatement ps_getFolderName = connection
				.prepareStatement("Select source_folder,folder_local from table_config");
		ResultSet rs_getFolderName = ps_getFolderName.executeQuery();
		while (rs_getFolderName.next()) {
			boolean exist = false;
			// *Get all name table in DW
			PreparedStatement ps_getAllTableFromDW = connection2.prepareStatement(
					"select TABLE_NAME from information_schema.TABLES where table_schema = 'database_warehouse'");
			ResultSet rs_getAllTableFromDW = ps_getAllTableFromDW.executeQuery();
			while (rs_getAllTableFromDW.next()) {
				// *If table have been exist don't create
				if (rs_getFolderName.getString(2).equals(rs_getAllTableFromDW.getString(1))) {
					exist = true;
				}
			}
			if (!exist) {
				// *Get all in source
				PreparedStatement ps_getFromSource = connection1
						.prepareStatement("Select * from " + rs_getFolderName.getString(1));
				ResultSet rs_getFromSource = ps_getFromSource.executeQuery();
				ResultSetMetaData rsmd_getFromSoure = rs_getFromSource.getMetaData();
				// *Get num of col in table source
				int num_of_col = rsmd_getFromSoure.getColumnCount();
				// *Create sql to create database
				String sql = "Create table ";
				sql += rs_getFolderName.getString(2) + "(";
				for (int i = 1; i < num_of_col + 1; i++) {
					if (i == num_of_col) {
						sql += rsmd_getFromSoure.getColumnName(i) + " varchar(255))";
						break;
					}
					String col_name = rsmd_getFromSoure.getColumnName(i);
					sql += col_name + " varchar(255),";
				}
				PreparedStatement ps_createDB = connection2.prepareStatement(sql);
				ps_createDB.executeUpdate();
				System.out.println("Success");
			} else {
				System.out.println("Database is already exist");
			}
		}
	}

	public void copyToDataWarehouse() throws ClassNotFoundException, IOException, SQLException {
		Connection connection = connect.loadProps();
		Connection connection1 = connect1.loadProps();
		Connection connection2 = connect2.loadProps();

		PreparedStatement ps_getTable_Name = connection
				.prepareStatement("Select id,source_folder,folder_local from table_config ");
		ResultSet rs_getTable_Name = ps_getTable_Name.executeQuery();
		while (rs_getTable_Name.next()) {
			// *Get status of log
			PreparedStatement ps_getStatus = connection.prepareStatement(
					"Select status from table_log where id = '" + rs_getTable_Name.getString(1) + "'");
			ResultSet rs_getStatus = ps_getStatus.executeQuery();
			while (rs_getStatus.next()) {
				// *Check status
				if (rs_getStatus.getString(1).equals("Download_OK")) {

					// *Get all of staging
					PreparedStatement ps_getFromStaging = connection1
							.prepareStatement("Select * from " + rs_getTable_Name.getString(2));
					ResultSet rs_getFromStaging = ps_getFromStaging.executeQuery();
					ResultSetMetaData rsmd_getFromStaging = rs_getFromStaging.getMetaData();
					int colOfStaging = rsmd_getFromStaging.getColumnCount();

					// *Get all of DW
					PreparedStatement ps_getFromDW = connection2
							.prepareStatement("Select * from " + rs_getTable_Name.getString(3));

					ResultSet rs_getFromDW = ps_getFromDW.executeQuery();
					ResultSetMetaData rsmd_getFromDW = rs_getFromDW.getMetaData();
					int colOfDW = rsmd_getFromDW.getColumnCount();
					// *Check col of 2 database
					if (colOfDW == colOfStaging) {
						String sql = "Insert into " + rs_getTable_Name.getString(3) + " values(";
						for (int i = 1; i < colOfDW + 1; i++) {
							if (i == colOfDW) {
								sql += "?)";
								break;
							}
							sql += "?,";
						}
						while (rs_getFromStaging.next()) {
							PreparedStatement ps_insertToDB = connection2.prepareStatement(sql);
							for (int i = 1; i < colOfDW + 1; i++) {
								ps_insertToDB.setString(i, rs_getFromStaging.getString(i));
							}
							ps_insertToDB.executeUpdate();
							PreparedStatement ps_delete = connection1.prepareStatement("Delete from "
									+ rs_getTable_Name.getString(2) + " where STT=" + rs_getFromStaging.getString(1));
							ps_delete.executeUpdate();
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
		d.createDatabase();
		d.copyToDataWarehouse();
	}
}
