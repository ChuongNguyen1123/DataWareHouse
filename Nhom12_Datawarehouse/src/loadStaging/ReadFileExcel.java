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

//Bước 3.Đọc file
public List<SinhVien> readFileFromExcelFile(String excelFilePath) throws IOException {
    List<SinhVien> listSV = new ArrayList<SinhVien>();
    //Lấy 1 file excel
    FileInputStream inputStream = new FileInputStream(new File(excelFilePath));
     //tạo workBook chỉ đến file excel
    Workbook workBook = getWorkbook(inputStream, excelFilePath);
    //Lấy ra sheet đầu tiên từ workBook
    Sheet firstSheet = workBook.getSheetAt(0);
    Row rows;
    //Lấy từ dòng thứ 2
    for(int i =1;i<=firstSheet.getLastRowNum(); i++) {
    	 rows = (Row) firstSheet.getRow(i);
    	 //Lấy ra  Iterator cho tất cả các cell của dòng hiện tại
        Iterator<Cell> cells = rows.cellIterator();
        SinhVien sinhvien = new SinhVien();       
        while (cells.hasNext()) {
            Cell cell = cells.next();
            int columnIndex = cell.getColumnIndex();
            //đọc dữ liệu 
            switch (columnIndex) {
                case 0:
                	sinhvien.setStt((String) getCellValue(cell));
                    break;
                case 1:
                	sinhvien.setMaSV((String) getCellValue(cell));
                    break;
                case 2:
                	sinhvien.setHoLot((String) getCellValue(cell));
                    break;
                case 3:
                	sinhvien.setTen((String) getCellValue(cell));
                    break;
                case 4:
                	sinhvien.setNgaySinh((String) getCellValue(cell));
                    break;
                case 5:
                	sinhvien.setMaLop((String) getCellValue(cell));
                    break;
                case 6:
                	sinhvien.setTenLop((String) getCellValue(cell));
                    break;
                case 7:
                	sinhvien.setDtLienLac((String) getCellValue(cell));
                    break;
                case 8:
                	sinhvien.setEmail((String) getCellValue(cell));
                    break;
                case 9:
                	sinhvien.setQueQuan((String) getCellValue(cell));
                    break;
                case 10:
                	sinhvien.setGhiChu((String) getCellValue(cell));
                    break;
            }
        }
        listSV.add(sinhvien);
    }
     
    workBook.close();
    inputStream.close();
     
    return listSV;
}
//Định dạng các giá trị trong file
private Object getCellValue(Cell cell) {
DataFormatter df = new DataFormatter();
//Tính toán giá trị ô cho bởi công thức đó
FormulaEvaluator fe = null;
switch (cell.getCellType()) {
//Định dạng chuỗi
    case STRING:
        return cell.getStringCellValue();
    case BOOLEAN:
        return cell.getBooleanCellValue();
        //Định dạng số
    case NUMERIC:
    	return df.formatCellValue(cell);
    	//đinh dạng công thức
    case FORMULA:
    	return df.formatCellValue(cell,fe);
}

return null;
}
 //được sử dụng để có thể  đọc được cả đinh dạng .xlsx và .xls
private static Workbook getWorkbook(FileInputStream inputStream, String excelFilePath) throws IOException {
    Workbook workbook = null;
  //Nếu đuôi file là .xlsx và .xls thì đọc file
    if (excelFilePath.endsWith("xlsx")) {
        workbook = new XSSFWorkbook(inputStream);
    } else if (excelFilePath.endsWith("xls")) {
        workbook = new HSSFWorkbook(inputStream);
        //Nếu không thì không đọc file
    } else {
        throw new IllegalArgumentException("Không thể đọc file");
    }
  
    return workbook;
}

    public static void main(String[] args) throws IOException, InvalidFormatException, ClassNotFoundException, SQLException {
    	//Kết nối database
    	ConnectionDB connect = new ConnectionDB();
	       Connection  connection = connect.loadProps();
	       //Lấy danh sách trong bảng log và config
		String sql = "SELECT *  from table_config c, databasecontroll.table_log l  where l.config_id = c.id ";
		
		PreparedStatement ps = connection.prepareStatement(sql);
		  //Láy giá trị trong database
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
		String file1 = rs.getString("folder_local") + "\\" + rs.getString("name_file");
		 List<SinhVien> listSV = new ReadFileExcel().readFileFromExcelFile(file1);
    		  System.out.println(listSV);

    
    }
}
}

