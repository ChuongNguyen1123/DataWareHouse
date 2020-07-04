package loadStaging ;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import javax.swing.text.StyledEditorKit.BoldAction;

import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import connectionDB.ConnectionDB;

public class ReadFileExcel {
	
public static  String readFile (String file1) throws IOException, ClassNotFoundException, SQLException {

		  File file = new File(file1);// mo file
			if (!file.exists()) {
				System.out.println("File :" + file1 + " Không tồn tại");}
			else {
//	  String file = "C:\\Users\\Admin\\Documents\\17130016_sang_nhom12.xlsx";
    // Creating a Workbook from an Excel file (.xls or .xlsx)
    Workbook workbook = WorkbookFactory.create(file);

   // Getting the Sheet at index zero
    Sheet sheet = workbook.getSheetAt(0);

    // Create a DataFormatter to format and get each cell's value as String
    DataFormatter dataFormatter = new DataFormatter();

    // 1. You can obtain a rowIterator and columnIterator and iterate over them

    Iterator<Row> rowIterator = sheet.rowIterator();
    while (rowIterator.hasNext()) {
        Row row = rowIterator.next();

        // Now let's iterate over the columns of the current row
        Iterator<Cell> cellIterator = row.cellIterator();

        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            String cellValue = dataFormatter.formatCellValue(cell);
            System.out.print(cellValue + "\t");
        }
        System.out.println();
    }

    workbook.close();
}
			return "";

}

    public static void main(String[] args) throws IOException, InvalidFormatException, ClassNotFoundException, SQLException {
    	String sql = "SELECT * FROM table_config";
        ConnectionDB connect = new ConnectionDB();
	       Connection  connection = connect.loadProps();
		PreparedStatement ps = connection.prepareStatement(sql);
    	ResultSet rs = ps.executeQuery();
    	while (rs.next()) {
    		
    		  String file1 = rs.getString("folder_local") + "\\" + rs.getString("file") ;
    		  
    		  
    		  
    	ReadFileExcel rd = new ReadFileExcel();
    System.out.println(rd.readFile(file1));
    
    }
}
}

