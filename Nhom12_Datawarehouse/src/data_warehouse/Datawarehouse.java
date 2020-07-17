package data_warehouse;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import connectionDB.ConnectionDB;
import connectionDB.ConnectionDB1;
import connectionDB.ConnectionDB2;

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
		PreparedStatement ps_getTableName = connection
				.prepareStatement("Select table_name_datawarehouse,field_name from my_config where isActive=1");
		ResultSet rs_getTableName = ps_getTableName.executeQuery();
		while (rs_getTableName.next()) {
			String sql = "Create table ";
			sql += rs_getTableName.getString(1) + "(";
			String[] result = rs_getTableName.getString(2).split(",");
			for (int i = 0; i < result.length; i++) {
				if (i == result.length - 1) {
					sql += result[i] + " varchar(255))";
					break;
				}
				sql += result[i] + " varchar(255),";
			}
			PreparedStatement ps_createDB = connection2.prepareStatement(sql);
			ps_createDB.executeUpdate();
			System.out.println("Success");
		}
	}

	public void copyToDataWarehouse() throws ClassNotFoundException, IOException, SQLException {
		Connection connection = connect.loadProps();
		Connection connection1 = connect1.loadProps();
		Connection connection2 = connect2.loadProps();
		PreparedStatement ps_getTable_Name = connection.prepareStatement(
				"Select table_name_staging,table_name_datawarehouse,insert_DW_sql,table_column from my_config ");
		ResultSet rs_getTable_Name = ps_getTable_Name.executeQuery();
		while (rs_getTable_Name.next()) {
			PreparedStatement ps_getStatus = connection
					.prepareStatement("Select status from log where name = '" + rs_getTable_Name.getString(1) + "'");
			ResultSet rs_getStatus = ps_getStatus.executeQuery();
			while (rs_getStatus.next()) {
				if (rs_getStatus.getString(1).equalsIgnoreCase("OK")) {
					PreparedStatement ps_getFromStaging = connection1
							.prepareStatement("Select * from " + rs_getTable_Name.getString(1));
					ResultSet rs_getFromStaging = ps_getFromStaging.executeQuery();
					while (rs_getFromStaging.next()) {
						PreparedStatement ps_insertToDB = connection2.prepareStatement(rs_getTable_Name.getString(3));
						for (int i = 0; i < rs_getTable_Name.getInt(4); i++) {
							ps_insertToDB.setString(i+1, rs_getFromStaging.getString(i+1));
						}
						ps_insertToDB.executeUpdate();
					}
				}

			}
		}
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, SQLException {
		Datawarehouse d = new Datawarehouse();
		// d.createDatabase();
		d.copyToDataWarehouse();
	}
}
