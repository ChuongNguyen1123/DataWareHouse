package download;

import java.awt.List;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Download {
	private String sid = null;
	private String urlHttp, usernameDriver, passwordDriver, srcFolderDriver, desSRCLocal, kieuFile;
	private String connectionURL, userName, passWord, sqlConfig;
	private String sqlLog;
	private Connection connection;


//	Load file config.properties de ket noi voi DB
	public Connection loadProps() throws IOException { 
		FileInputStream f = new FileInputStream("src/dms_config.properties"); // duong dan den file lay cau hinh ket noi DB
		Properties pros = new Properties();
		pros.load(f); 
		this.connectionURL = pros.getProperty("connectionURL"); // duong dan ket noi den Navicat lay tu file dms_config
		this.userName = pros.getProperty("userName");		 // username dang nhap DB
		this.passWord = pros.getProperty("passWord");		// password dang nhap DB
		this.sqlConfig = pros.getProperty("sqlconfig");		// cau query lay du lieu bang config
		this.sqlLog = pros.getProperty("sqllog");		// cau query lay du lieu bang Log
		try {
//			Thiet lap ket noi voi DB qua duong dan url, username, password or tren 
			connection = DriverManager.getConnection(connectionURL, userName, passWord);  
			String lenhSQL = sqlConfig;
			PreparedStatement pre = connection.prepareStatement(lenhSQL);  // truy van den cac tham so cua cau query lenhSQL
			ResultSet resultSet = pre.executeQuery();  // Lay ra cac dong cua table config
			while (resultSet.next()) { 		// duyet tung dong cua table config
				urlHttp = resultSet.getString("url");   	// Lay du lieu cua cot url tu table config
				usernameDriver = resultSet.getString("username");		// Lay  username tu table config de login den server
				passwordDriver = resultSet.getString("password");		// Lay  password tu table config de login den server
				srcFolderDriver = resultSet.getString("source_folder");		// Lay  duong dan den thu muc can download du lieu
				desSRCLocal = resultSet.getString("folder_local");		// Lay  duong dan den thu muc duoi local
				kieuFile = resultSet.getString("file");			// loai file download
			}

		} catch (SQLException e) {
			SendMailSSL senmail = new SendMailSSL(); 		// goi lop senmail
//			senmai (chu de mail, noi dung gui)
			senmail.sendMail("Data Warehouse nhom 12 - Ca sang ", " Khong the ket noi vao DB");
			System.out.println("Loi ket noi, kiem tra lai");  // in ra man hinh
			System.exit(0);
			e.printStackTrace();
		}
		return connection;
	}

//	Dang nhap den server Goi API SYNO.API.Auth API version 3
	public void login() {
		try {
//			Duong dan de dung API login vao server 
//			sao khi login thanh cong se tra ra chuoi token
			URL urlForGetRequest = new URL(
					urlHttp + "/webapi/auth.cgi?api=SYNO.API.Auth&version=3&method=login&account=" + usernameDriver
							+ "&passwd=" + passwordDriver + "&session=FileStation&format=cookie");
//				ket noi toi mot URL ma protocol cua no la HTTP
			HttpURLConnection conectHttpURL = (HttpURLConnection) urlForGetRequest.openConnection();
			conectHttpURL.setRequestMethod("GET");  //	phuong thuc gui yeu cau la get
			int responseCode = conectHttpURL.getResponseCode();   //	Lay duoc cai trang thai cua requet
			if (responseCode == HttpURLConnection.HTTP_OK) {   //	ket noi thanh cong
				BufferedReader in = new BufferedReader(new InputStreamReader(conectHttpURL.getInputStream()));
				StringBuffer response = new StringBuffer();
				String line = null;
				while ((line = in.readLine()) != null) {
					response.append(line);
				}
				in.close();
//  khai bao thu vien Json dung de lay du lieu tra ve
				JSONParser parser = new JSONParser();
				JSONObject jsonObject = (JSONObject) parser.parse(response.toString());
				this.sid = (String) ((JSONObject) jsonObject.get("data")).get("sid");  //	lay token gan cho bien sid
				// print chuoi token khi login thanh cong
				System.out.println("sid: " + sid);
				// GetAndPost.POSTRequest(response.toString());
			} else {
				System.out.println("GET NOT WORKED");
			}
		} catch (Exception e) {
//			gui mail khi login kh thanh cong
			SendMailSSL senmail = new SendMailSSL();
			senmail.sendMail("Data Warehouse nhom 12 - Ca sang ", " Khong the login vao he thong tai du lieu!");
			System.out.println("Not Login!");
		}

	}

	
//	Phuong thuc lay ngay thang nam hien tai
	public static String getTime() {
		DateFormat df = new SimpleDateFormat("yyyy/MM/dd -- HH:mm:ss");   //Khoi tao doi tuong format ngay thang nam cua java
		Date date = new Date();       //Doi tuong ngay
//		System.out.println(df.format(date));
		return df.format(date);

	}

//	Lay list file o thu muc tren server truyen vao ten loai file va kieu file
	public LinkedList<String> listFiles(String nameFile, String typeFile) throws Exception {
		LinkedList<String> listFileSource = new LinkedList<String>();
//		Lay list file o thu muc tren server dung API SYNO.FileStation.List version 1
			URL urlForGetRequest = new URL(
					urlHttp + "/webapi/entry.cgi?api=SYNO.FileStation.List&version=1&method=list&folder_path="
							+ srcFolderDriver + "&_sid=" + sid);
			HttpURLConnection conectHttpURL = (HttpURLConnection) urlForGetRequest.openConnection();
			conectHttpURL.setRequestMethod("GET");
			int responseCode = conectHttpURL.getResponseCode();
			if (responseCode == HttpURLConnection.HTTP_OK) {
				BufferedReader in = new BufferedReader(new InputStreamReader(conectHttpURL.getInputStream()));
				StringBuffer response = new StringBuffer();
				String line = null;
				while ((line = in.readLine()) != null) {
					response.append(line);
				}
				in.close();
				JSONParser parser = new JSONParser();
				JSONObject jsonObject = (JSONObject) parser.parse(response.toString());
//			Lay tat ca cac file tim thay dan vao doi tuong mang jsonArray
				JSONArray files = (JSONArray) ((JSONObject) jsonObject.get("data")).get("files");
				for (int i = 0; i < files.size(); i++) {
//					lay ra duong dan den file tren server
					String pathFile = (String) ((JSONObject) files.get(i)).get("path");
//					cat duong dan de lay ten file
					String nameFilesrc = pathFile.substring(pathFile.lastIndexOf("/") + 1);
//					Dem ten file so sanh dung loai file va kieu file de download ve local
					if (nameFilesrc.contains(nameFile) && nameFilesrc.contains(typeFile)) {
//						tat ca ca file dung yeu cau se duoc them ca duong dan den file do vao mang listFileSouce 
						listFileSource.push((String) ((JSONObject) files.get(i)).get("path"));
					}
			}

		}
//	tra ve mang cac file dung dinh dang tren server
		return listFileSource;
	}

	
	
//	Check sum MD5 de kiem tra file tren server
//	phuong thuc nhan cao duong dan den file tren server
	public String checkSumMD5FileSrc(String srcFile) throws IOException, ParseException {
		String taskid = ""; 	//	chuoi ket qua sau khi checksum
//		doi API md5 la SYNO.FileStation.MD5 version 1 de check sum file 
		URL urlForGetRequest = new URL(
				urlHttp + "/webapi/entry.cgi?api=SYNO.FileStation.MD5&version=1&method=start&file_path=" + srcFile
						+ "&_sid=" + sid);
		HttpURLConnection conectHttpURL = (HttpURLConnection) urlForGetRequest.openConnection();
		conectHttpURL.setRequestMethod("GET");
		int responseCode = conectHttpURL.getResponseCode();
		if (responseCode == HttpURLConnection.HTTP_OK) {
			BufferedReader in = new BufferedReader(new InputStreamReader(conectHttpURL.getInputStream()));
			StringBuffer response = new StringBuffer();
			String line = null;
			while ((line = in.readLine()) != null) {
				response.append(line);
			}
			in.close();
			JSONParser parser = new JSONParser();
			JSONObject jsonObject = (JSONObject) parser.parse(response.toString());
//			lay ra ket qua checksum duoc gan vao chuoi checksum khai bao ow tren
			taskid = (String) ((JSONObject) jsonObject.get("data")).get("taskid");
		}
		return taskid; 
	}

//	Phuong thuc down file tren server
	public void down() throws Exception {
//		goi ham doc file de ket noi voi DB
		loadProps();
		if (connection != null) {
//		ket noi thanh cong thi lay du lieu tu table config de dang nhap vao server
			login();
			if (sid != null) {
//				login thanh cong
//				tien hanh lay tat ca cac dong cua file config
				String sql_selectConfig = "SELECT * FROM table_config";
				PreparedStatement pre_selectConfig = connection.prepareStatement(sql_selectConfig);
				ResultSet rs_selectConfig = pre_selectConfig.executeQuery();
//				duyet down tung dong config
				while (rs_selectConfig.next()) {
//					lay ra du lieu cot id, table_name, file cua table config
					Integer idConfig = rs_selectConfig.getInt("id");
					String nameFileConfig = rs_selectConfig.getString("table_name");
					String typeFileConfig = rs_selectConfig.getString("file");
//		voi moi dong config tuong ung voi mot loai file
//		Tim tren server nhung file thuoc loai file do dem gan vao mang moi goi la listFileSrc
					LinkedList<String> listFileSrc = listFiles(nameFileConfig, typeFileConfig);
//					duyet mang file de down load tung file con ve
					for (int i = 0; i < listFileSrc.size(); i++) {
//						Truoc khi down can phai check file do da duoc down hay chua 
						String srcFileCheck = checkFile(listFileSrc.get(0), idConfig);  //	goi ham checkfile de kiem tra 
						if (srcFileCheck != null) {  //	file tren server la file moi hoac file co su thay doi
							String nameFile = srcFileCheck.substring(srcFileCheck.lastIndexOf("/") + 1);  // lay ra ten file tren server 
//			goi API download file ve local la SYNO.FileStation.Download version 1
							URL urlForGetRequest = new URL(urlHttp
									+ "/webapi/entry.cgi?api=SYNO.FileStation.Download&version=1&method=download&mode=open&path="
									+ srcFileCheck + "&_sid=" + sid);
							HttpURLConnection conectHttpURL = (HttpURLConnection) urlForGetRequest.openConnection();
							conectHttpURL.setRequestMethod("GET");
							int responseCode = conectHttpURL.getResponseCode();
							if (responseCode == HttpURLConnection.HTTP_OK) {
								InputStream in = new BufferedInputStream((conectHttpURL.getInputStream()));

								BufferedOutputStream out = new BufferedOutputStream(
										new FileOutputStream(desSRCLocal + "\\" + nameFile));
								int readData;
								byte[] buff = new byte[1024];
//  qua trinh doc file tu server ghi xuong lacal
								while ((readData = in.read(buff)) > -1) {
									out.write(buff, 0, readData);
								}
//								Sau khi doc ghi xong tien hanh dong file
								in.close();
								out.close();
							}

						}
					}
				}
			}
//			Duyet cac file down ve thanh cong duoi thu muc lacal
			File file = new File(desSRCLocal);
			File[] listFileLocal = file.listFiles();
			String status = "Download_OK";  //	gan status cho file co duoi local
			for (File file2 : listFileLocal) {
//				cau query updat vao bang log
				String sqlUdateLog = "UPDATE table_log SET date_download = ?, status = ? WHERE name_file = '"+ file2.getName() + "'";
				PreparedStatement prepaUpdateLog = connection.prepareStatement(sqlUdateLog);
				prepaUpdateLog.setString(1, getTime());
				prepaUpdateLog.setString(2, status);
				prepaUpdateLog.executeUpdate();

				
			}
			connection.close();  //	dong ket noi DB
		}
	}

	
//	Kiem tra file tren server voi duoi local
	public String checkFile(String pathNameFileSrc, int idConfig) throws Exception {
		String srcFileCheck = "";
		String status = "EOR";  //	set status nhung file tren local chua duoc down
		String checkMD5 = checkSumMD5FileSrc(pathNameFileSrc); //	goi ham checksum file server o tren
		String nameFileLog = pathNameFileSrc.substring(pathNameFileSrc.lastIndexOf("/") + 1);  // cat namfileSRC thanh namfile
//		cau query la du lie cua cot log the cot name_file
		String select_log = "SELECT * FROM table_log WHERE name_file = '" + nameFileLog + "'";
		PreparedStatement preSelect_log = connection.prepareStatement(select_log);
		ResultSet rsSelect_log = preSelect_log.executeQuery();
//		Duyet du lieu tu dong cua table log
		if (!rsSelect_log.next()) {  //	Truong hop down ve lan dau thi table log chua co gi hoac file co su thay doi tren server
			srcFileCheck = pathNameFileSrc;
//			Cau query them row vao bang Table_Log
			String insertFileLog = "INSERT INTO table_log(source_folder, name_file, status, md5, config_id) VALUES (?, ?, ?, ?, ?)";
			PreparedStatement preIsFileLog = connection.prepareStatement(insertFileLog);
			preIsFileLog.setString(1, urlHttp + srcFolderDriver);
			preIsFileLog.setString(2, nameFileLog);
			preIsFileLog.setString(3, status);
			preIsFileLog.setString(4, checkMD5);
			preIsFileLog.setInt(5, idConfig);
			preIsFileLog.execute();
		} else { 	//	neu cung ten ma giong nhau cai MD5
			if (rsSelect_log.getString("md5").equals(checkMD5)) {
				srcFileCheck = null; // bo qua file tren server do
			} else {  //	neu cung ten ma khac nhau cai MD5
				srcFileCheck = pathNameFileSrc;  
//				Cau query de cap nhat vao cot table log
				String updateLog = "UPDATE table_log SET md5 = '" + checkMD5 + "', status = '" + status
						+ "' WHERE name_file = '" + nameFileLog + "'";
				PreparedStatement preUpdateLog = connection.prepareStatement(updateLog);
				preUpdateLog.executeUpdate();
			}
		}
		return srcFileCheck;  //	xuat ra duong dan den file do tren server
	}

//	public static void main(String[] args) throws Exception {
//		Download test = new Download();
//		test.down();
//	}

}
