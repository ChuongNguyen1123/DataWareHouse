package extract;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.mysql.jdbc.Connection;
public class ReadFileCSV {
	public static ArrayList<String> loadDataCSV(String fileLocalPath,String dataFileType,String dataFileName) throws ClassNotFoundException, SQLException {
		ArrayList<String> listData = new ArrayList<String>();
		File dataFile = new File(fileLocalPath + "\\" + dataFileName + dataFileType);
		if (!dataFile.exists()) {
			System.out.println("File không tồn tại!");
		} else {
			BufferedReader br = null;
			try {
			FileInputStream fin = new FileInputStream( dataFile);
			
			XSSFWorkbook workbook = new XSSFWorkbook(fin);
			XSSFSheet sheet = workbook.getSheetAt(0);
			Iterator<Row> rowIt = sheet.iterator();
			Connection con = null;
			while (rowIt.hasNext()) {
				Row row = rowIt.next();
				Iterator<Cell> cellIt = row.iterator();
				while (cellIt.hasNext()) {
					Cell cell = cellIt.next();
					System.out.println(cell.toString()+"\t");
				
			}
				System.out.println();
		}
			workbook.close();
			fin.close();}
		 catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	return listData;
}

	

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		String fileLocalPath = "C:\\Users\\Admin\\Documents";
		String dataFileType = ".csv";
		String dataFileName = "17130016_sang_nhom12";
		ArrayList<String> dataList = ReadFileCSV.loadDataCSV(fileLocalPath, dataFileType, dataFileName);
		for (String dataLine : dataList) {
			System.out.println(dataLine);
		}
	}
}
