package data_warehouse;

import java.io.IOException;
import java.sql.*;

import connectionDB.ConnectionDB;

public class DataProcessing {
	static String NAME_DB = "student";
	static String LIST_DB = "name_of_database";
	static String DB_DES = "newstudent";
	ConnectionDB connect = new ConnectionDB();

	// *We get so much DB in MySql to copy.
	// *Add name of DB to another Database
	public void addNameData() throws SQLException, IOException, ClassNotFoundException {
		Connection connection = connect.loadProps();
		// *get all name in database
		PreparedStatement ps_showTable = connection.prepareStatement("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + NAME_DB + "'");
		ResultSet rs_showTable = ps_showTable.executeQuery();
		// *get Name of table from ListDB to compare
		PreparedStatement ps_getNameInListDBToCompare = connection.prepareStatement("SELECT Name From " + LIST_DB);
		ResultSet rs_getNameInListDBToCompare = ps_getNameInListDBToCompare.executeQuery();
		while (rs_showTable.next()) {
//			while (rs_getNameInListDBToCompare.next()) {
				 //*don't be the same with List database and database des
				
				if ( !rs_showTable.getString(1).equalsIgnoreCase(DB_DES) && !rs_showTable.getString(1).equalsIgnoreCase(LIST_DB)) {
					// *add into listDB
					PreparedStatement ps_copyNameToAnotherDB = connection.prepareStatement("Insert into " + LIST_DB + " values(?)");
					ps_copyNameToAnotherDB.setString(1, rs_showTable.getString(1));
					ps_copyNameToAnotherDB.executeUpdate();
				}
		}
		}
//	}

	// *Check col at first if col != between source and des it is wrong.
	public void checkSourcDB() throws SQLException, IOException, ClassNotFoundException {
		Connection connection = connect.loadProps();
		// select name of database
		PreparedStatement ps_getNameOfListDB = connection.prepareStatement("Select Name from " + LIST_DB);
		ResultSet rs_getNameOfListDB = ps_getNameOfListDB.executeQuery();
		// select name
		while (rs_getNameOfListDB.next()) {
			// get data from name database
			PreparedStatement ps_getDBFromList = connection
					.prepareStatement("Select * from " + rs_getNameOfListDB.getString(1));
			ResultSet rs_getDBFromList = ps_getDBFromList.executeQuery();
			// *create object to count col of database source
			ResultSetMetaData rsmd1 = rs_getDBFromList.getMetaData();
			int numOfCol1 = rsmd1.getColumnCount();

			// get col database Des
			PreparedStatement ps_getDBDes = connection.prepareStatement("Select * from " + DB_DES);
			ResultSet rs_getDBDes = ps_getDBDes.executeQuery();
			// *create object to count col of database source
			ResultSetMetaData rsmd2 = rs_getDBDes.getMetaData();
			int numOfCol2 = rsmd2.getColumnCount();
			if (numOfCol1 == numOfCol2) {
				String nameOfCol1 = "";
				String nameOfCol2 = "";
				String ColTypeName1 = "";
				String ColTypeName2 = "";
				// count col same(name,type)
				int count = 0;
				for (int i = 1; i < numOfCol1 + 1; i++) {
					// *get name of col
					nameOfCol1 = rsmd1.getColumnName(i);
					nameOfCol2 = rsmd2.getColumnName(i);
					// *get type of col
					ColTypeName1 = rsmd1.getColumnTypeName(i);
					ColTypeName2 = rsmd2.getColumnTypeName(i);
					if (nameOfCol1.equals(nameOfCol2) && ColTypeName1.equals(ColTypeName2)) {
						count++;
					}
				}
				// *don't have active is not approved
				// *active = 1 is can copy
				// *active = 2 is can't copy
				if (count == numOfCol1) {
					PreparedStatement ps_createActive1 = connection.prepareStatement(
							"ALTER TABLE " + rs_getNameOfListDB.getString(1) + " ADD Active int(1) NOT NULL");
					ps_createActive1.executeUpdate();
					PreparedStatement ps_setActive1 = connection
							.prepareStatement("UPDATE " + rs_getNameOfListDB.getString(1) + " SET Active=1");
					ps_setActive1.executeUpdate();
				} else {
					PreparedStatement ps_createActive2 = connection.prepareStatement(
							"ALTER TABLE " + rs_getNameOfListDB.getString(1) + " ADD Active int(1) NOT NULL");
					ps_createActive2.executeUpdate();
					PreparedStatement ps_setActive2 = connection
							.prepareStatement("UPDATE " + rs_getNameOfListDB.getString(1) + " SET Active=2");
					ps_setActive2.executeUpdate();
				}
			}
		}
	}

	public void createTable(String name) throws SQLException, IOException, ClassNotFoundException {
		Connection connection = connect.loadProps();
		String ct = "CREATE TABLE " + name
				+ "( Stt int NOT NULL PRIMARY KEY AUTO_INCREMENT,MaSV int, Ho VARCHAR(20), Ten VARCHAR(20), NgaySinh date, MaLop VARCHAR(20), TenLop VARCHAR(20), SoDT int, Email VARCHAR(20), QueQuan VARCHAR(20), GhiChu VARCHAR(20))";
		System.out.println(ct);
		PreparedStatement ps_createTable = connection.prepareStatement(ct);
		ps_createTable.executeUpdate();
	}

	public void copyFromThisDBToAnotherDB() throws SQLException, IOException, ClassNotFoundException {
		Connection connection = connect.loadProps();
		// *Select name of Database
		PreparedStatement ps_getNameOfListDB = connection.prepareStatement("Select Name from " + LIST_DB);
		ResultSet rs_getNameOfListDB = ps_getNameOfListDB.executeQuery();
		while (rs_getNameOfListDB.next()) {
			// get data from source
			PreparedStatement ps_getDBFromList = connection
					.prepareStatement("Select * From " + rs_getNameOfListDB.getString(1) + " WHERE Active=1");
			ResultSet rs_getDBFromList = ps_getDBFromList.executeQuery();
			PreparedStatement ps_copy = connection
					.prepareStatement("Insert into " + DB_DES + " VALUES(?,?,?,?,?,?,?,?,?,?,?)");
			while (rs_getDBFromList.next()) {
				ps_copy.setInt(1, 0);
				ps_copy.setInt(2, rs_getDBFromList.getInt(2));
				if(rs_getDBFromList.getString(3) == null){
					ps_copy.setString(3, "Null");
				}else{
				ps_copy.setString(3, rs_getDBFromList.getString(3));
				}
				
				if(rs_getDBFromList.getString(4) == null){
					ps_copy.setString(4, "Null");
				}else{
				ps_copy.setString(4, rs_getDBFromList.getString(4));
				}
				
				if(rs_getDBFromList.getString(6) == null){
					ps_copy.setString(6, "Null");
				}else{
				ps_copy.setString(6, rs_getDBFromList.getString(6));
				}
				if(rs_getDBFromList.getString(7) == null){
					ps_copy.setString(7, "Null");
				}else{
				ps_copy.setString(7, rs_getDBFromList.getString(7));
				}
				if(rs_getDBFromList.getString(9) == null){
					ps_copy.setString(9, "Null");
				}else{
				ps_copy.setString(9, rs_getDBFromList.getString(9));
				}
				if(rs_getDBFromList.getString(10) == null){
					ps_copy.setString(10, "Null");
				}else{
				ps_copy.setString(10, rs_getDBFromList.getString(10));
				}
				if(rs_getDBFromList.getString(11) == null){
					ps_copy.setString(11, "Null");
				}else{
				ps_copy.setString(11, rs_getDBFromList.getString(11));
				}
				ps_copy.setDate(5, rs_getDBFromList.getDate(5));
				ps_copy.setInt(8, rs_getDBFromList.getInt(8));
				ps_copy.execute();
				PreparedStatement ps_setActive3 = connection.prepareStatement("UPDATE " + rs_getNameOfListDB.getString(1) + " SET Active=3");
				ps_setActive3.executeUpdate();
			}
		}
	}

	public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
		DataProcessing dp = new DataProcessing();
//		 dp.createTable("student2");
		dp.addNameData();
		dp.checkSourcDB();
		dp.copyFromThisDBToAnotherDB();
	}
}
