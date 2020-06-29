package tool;

public class ExchangeJavaPath {
	public static String exchangeToJavaPath(String oldPath) {
		String javaPath = oldPath.replace("/", "\\\\");
		return javaPath;
	}
	public static void main(String[] args) {
		System.out.println(exchangeToJavaPath("C:/Users/Admin/Documents/"));
	}
}
