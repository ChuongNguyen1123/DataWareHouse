package log;

public class LogSTT {
	public static final String DF = "DF"; // download file fail
	public static final String DS = "DS"; // download file successfully
	public static final String ER = "ER"; // extract ready
	public static final String EF = "EF"; // extract failed
	public static final String ES = "ES"; // extracted to staging successfully
	public static final String TR = "TR"; // transform ready
	public static final String TS = "TD"; // transformed successfully
	public static final String LR = "LR"; // load file to data warehouse ready
	public static final String SU = "SU"; // load file to data warehouse successfully
	
	public static void main(String[] args) {
		System.out.println(LogSTT.DF);
	}
}
