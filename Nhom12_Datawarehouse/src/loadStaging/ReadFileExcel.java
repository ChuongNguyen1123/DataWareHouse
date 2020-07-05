package loadStaging ;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import org.apache.commons.compress.archivers.dump.InvalidFormatException;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;



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
private Object getCellValue(Cell cell) {
    switch (cell.getCellType()) {
        case STRING:
            return cell.getStringCellValue();
  
        case BOOLEAN:
            return cell.getBooleanCellValue();
            
 
    
    }
  
    return null;
}
public List<SinhVien> readBooksFromExcelFile(String excelFilePath) throws IOException {
    List<SinhVien> listBooks = new ArrayList<SinhVien>();
    FileInputStream inputStream = new FileInputStream(new File(excelFilePath));
     
    Workbook workBook = getWorkbook(inputStream, excelFilePath);
    Sheet firstSheet = workBook.getSheetAt(0);
    Iterator<Row> rows = firstSheet.iterator();
     
    while (rows.hasNext()) {
        Row row = rows.next();
        Iterator<Cell> cells = row.cellIterator();
        SinhVien book = new SinhVien();
         
        while (cells.hasNext()) {
            Cell cell = cells.next();
            int columnIndex = cell.getColumnIndex();
             
            switch (columnIndex) {
                case 0:
                    book.setStt((String) getCellValue(cell));
                    break;
                case 1:
                    book.setMaSV((String) getCellValue(cell));
                    break;
                case 2:
                    book.setHoLot((String) getCellValue(cell));
                    break;
                case 3:
                    book.setTen((String) getCellValue(cell));
                    break;
                case 4:
                    book.setNgaySinh((String) getCellValue(cell));
                    break;
                case 5:
                    book.setMaLop((String) getCellValue(cell));
                    break;
                case 6:
                    book.setTenLop((String) getCellValue(cell));
                    break;
                case 7:
                    book.setDtLienLac((String) getCellValue(cell));
                    break;
                case 8:
                    book.setEmail((String) getCellValue(cell));
                    break;
                case 9:
                    book.setQueQuan((String) getCellValue(cell));
                    break;
                case 10:
                    book.setGhiChu((String) getCellValue(cell));
                    break;
            }
        }
        listBooks.add(book);
    }
     
    workBook.close();
    inputStream.close();
     
    return listBooks;
}
 
 
private static Workbook getWorkbook(FileInputStream inputStream, String excelFilePath) throws IOException {
    Workbook workbook = null;
  
    if (excelFilePath.endsWith("xlsx")) {
        workbook = new XSSFWorkbook(inputStream);
    } else if (excelFilePath.endsWith("xls")) {
        workbook = new HSSFWorkbook(inputStream);
    } else {
        throw new IllegalArgumentException("The specified file is not Excel file");
    }
  
    return workbook;
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

