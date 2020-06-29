package extract;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
 
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class ReadFileExcel {
	public static final int COLUMN_INDEX_STT = 0;
    public static final int COLUMN_INDEX_MASV = 1;
    public static final int COLUMN_INDEX_HOLOT = 2;
    public static final int COLUMN_INDEX_TEN = 3;
    public static final int COLUMN_INDEX_NGAYSINH = 4;
    public static final int COLUMN_INDEX_MALOP = 5;
    public static final int COLUMN_INDEX_TENLOP = 6;
    public static final int COLUMN_INDEX_DTLIENLAC = 7;
    public static final int COLUMN_INDEX_EMAIL = 8;
    public static final int COLUMN_INDEX_QUEQUAN = 9;
    public static final int COLUMN_INDEX_GHICHU = 10;
 
    public static void main(String[] args) throws IOException {
        final String excelFilePath = "C:\\Users\\Admin\\Documents\\17130016_sang_nhom12.xlsx";
        final List<SinhVien> ds_sv = readExcel(excelFilePath);
        for (SinhVien sv : ds_sv) {
            System.out.println(sv);
        }
    }
 
    public static List<SinhVien> readExcel(String excelFilePath) throws IOException {
        List<SinhVien> ds_sv = new ArrayList<>();
 
        // Get file
        InputStream inputStream = new FileInputStream(new File(excelFilePath));
 
        // Get workbook
        Workbook workbook = getWorkbook(inputStream, excelFilePath);
 
        // Get sheet
        Sheet sheet = workbook.getSheetAt(0);
 
        // Get all rows
        Iterator<Row> iterator = sheet.iterator();
        while (iterator.hasNext()) {
            Row nextRow = iterator.next();
            if (nextRow.getRowNum() == 0) {
                // Ignore header
                continue;
            }
 
            // Get all cells
            Iterator<Cell> cellIterator = nextRow.cellIterator();
 
            // Read cells and set value for book object
            SinhVien sv = new SinhVien();
            while (cellIterator.hasNext()) {
                //Read cell
                Cell cell = cellIterator.next();
                Object cellValue = getCellValue(cell);
                if (cellValue == null || cellValue.toString().isEmpty()) {
                    continue;
                }
                // Set value for book object
                int columnIndex = cell.getColumnIndex();
                switch (columnIndex) {
                case COLUMN_INDEX_STT:
                    sv.setStt(new BigDecimal((double) cellValue).intValue());
                    break;
                case COLUMN_INDEX_MASV:
                    sv.setMaSV(new BigDecimal((double) cellValue).intValue());
                    break;
                case COLUMN_INDEX_HOLOT:
                    sv.setHoLot((String) getCellValue(cell));
                    break;
                case COLUMN_INDEX_TEN:
                    sv.setTen((String) getCellValue(cell));
                    break;
                case COLUMN_INDEX_NGAYSINH:
                    sv.setNgaySinh((String) getCellValue(cell));
                    break;
                case COLUMN_INDEX_MALOP:
                    sv.setMaLop((String) getCellValue(cell));
                    break;
                case COLUMN_INDEX_TENLOP:
                    sv.setTenLop((String) getCellValue(cell));
                    break;
                case COLUMN_INDEX_DTLIENLAC:
                    sv.setDtLienLac((String) getCellValue(cell));
                    break;
                case COLUMN_INDEX_EMAIL:
                    sv.setEmail((String) getCellValue(cell));
                    break;
                case COLUMN_INDEX_QUEQUAN:
                    sv.setQueQuan((String) getCellValue(cell));
                    break;
                case COLUMN_INDEX_GHICHU:
                    sv.setGhiChu((String) getCellValue(cell));
                    break;
                default:
                    break;
                }
 
            }
            ds_sv.add(sv);
        }
 
        workbook.close();
        inputStream.close();
 
        return ds_sv;
    }
 
    // Get Workbook
    private static Workbook getWorkbook(InputStream inputStream, String excelFilePath) throws IOException {
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
 
    // Get cell value
    private static Object getCellValue(Cell cell) {
        CellType cellType = cell.getCellTypeEnum();
        Object cellValue = null;
        switch (cellType) {
        case BOOLEAN:
            cellValue = cell.getBooleanCellValue();
            break;
        case FORMULA:
            Workbook workbook = cell.getSheet().getWorkbook();
            FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
            cellValue = evaluator.evaluate(cell).getNumberValue();
            break;
        case NUMERIC:
            cellValue = cell.getNumericCellValue();
            break;
        case STRING:
            cellValue = cell.getStringCellValue();
            break;
        case _NONE:
        case BLANK:
        case ERROR:
            break;
        default:
            break;
        }
 
        return cellValue;
    }
}
