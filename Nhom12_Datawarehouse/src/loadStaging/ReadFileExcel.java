package loadStaging ;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;



import connectionDB.ConnectionDB;

public class ReadFileExcel {
public static String readfile(String excelFilePath ) throws IOException {
	  FileInputStream inputStream = new FileInputStream(new File(excelFilePath));
	     
	    Workbook workBook = getWorkbook(inputStream, excelFilePath);
	    Sheet firstSheet = workBook.getSheetAt(0);
	    Row rows;
	    for(int i =1;i<=firstSheet.getLastRowNum(); i++) {
	    	 rows = (Row) firstSheet.getRow(i);
//	    Iterator<Row> rows = firstSheet.iterator();
//	     
//	    while (rows.hasNext()) {
//	        Row row = rows.next();
	        Iterator<Cell> cells = rows.cellIterator();
	        SinhVien book = new SinhVien();
//	         
	        while (cells.hasNext()) {
	            Cell cell = cells.next();
	            CellType columnIndex = cell.getCellType();
	        	DataFormatter df = new DataFormatter();
	            switch (columnIndex) {
				case STRING:
				System.out.println(cell.getStringCellValue() ) ;
				break;
		       case BOOLEAN:
		    	   System.out.println(cell.getStringCellValue() ) ;
		    	   break;
		        case NUMERIC:
		        	System.out.println(df.formatCellValue(cell)) ;
		        	break;
	            }
	            
	            }
//	FileInputStream inputStream = new FileInputStream(new File(excelFilePath));
//	XSSFWorkbook wb = new XSSFWorkbook(inputStream);
//	XSSFSheet sheet = wb.getSheetAt(0);
//	DataFormatter df = new DataFormatter();
//	for(Row row : sheet) {
//		for(Cell cell : row) {
//			switch (cell.getCellType()) {
//			case STRING:
//				System.out.println(cell.getStringCellValue() + "\t\t") ;
//				break;
//		       case BOOLEAN:
//		    	   System.out.println(cell.getStringCellValue() + "\t\t") ;
//		    	   break;
//		        case NUMERIC:
//		        	System.out.println(df.formatCellValue(cell) + "\t\t") ;
//		        	break;
//
//
//			}
//		}
//	}
	    }
	return excelFilePath;
}
private Object getCellValue(Cell cell) {
	DataFormatter df = new DataFormatter();
	FormulaEvaluator fe = null;
    switch (cell.getCellType()) {
        case STRING:
            return cell.getStringCellValue();
  
        case BOOLEAN:
            return cell.getBooleanCellValue();
        case NUMERIC:
        	return df.formatCellValue(cell);
        case FORMULA:
        	return df.formatCellValue(cell,fe);
    }
  
    return null;
}
public List<SinhVien> readBooksFromExcelFile(String excelFilePath) throws IOException {
    List<SinhVien> listBooks = new ArrayList<SinhVien>();
    FileInputStream inputStream = new FileInputStream(new File(excelFilePath));
     
    Workbook workBook = getWorkbook(inputStream, excelFilePath);
    Sheet firstSheet = workBook.getSheetAt(0);
    Row rows;
    for(int i =1;i<=firstSheet.getLastRowNum(); i++) {
    	 rows = (Row) firstSheet.getRow(i);
//    Iterator<Row> rows = firstSheet.iterator();
//     
//    while (rows.hasNext()) {
//        Row row = rows.next();
        Iterator<Cell> cells = rows.cellIterator();
        SinhVien book = new SinhVien();
//         
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
    	ConnectionDB connect = new ConnectionDB();
	       Connection  connection = connect.loadProps();
		String sql = "SELECT * FROM demo1.table_config";
		PreparedStatement ps = connection.prepareStatement(sql);
//		 ps.setInt(1, id);
		  
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
		String file1 = rs.getString(5) + "\\" + rs.getString(7);
		 List<SinhVien> listBooks = new ReadFileExcel().readBooksFromExcelFile(file1);
    		  System.out.println(file1);
    		  
    		  
    	ReadFileExcel rd = new ReadFileExcel();
//    System.out.println(listBooks);
    
    rd.readfile(file1);
    
    }
}
}

