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

import connectionDB.ConnectionDB;

public class Download {
	private String sid = null;
	private String urlHttp, usernameDriver, passwordDriver, srcFolderDriver, desSRCLocal;

//	Dang nhap den server Goi API SYNO.API.Auth API version 3
	public void login(String urlHttp, String usernameDriver, String passwordDriver) {
		try {
//			Duong dan de dung API login vao server sao khi login thanh cong se tra ra chuoi dang nhu token
			URL urlForGetRequest = new URL(
					urlHttp + "/webapi/auth.cgi?api=SYNO.API.Auth&version=3&method=login&account=" + usernameDriver
							+ "&passwd=" + passwordDriver + "&session=FileStation&format=cookie");
//				ket noi toi mot URL ma protocol cua no la HTTP
			HttpURLConnection conectHttpURL = (HttpURLConnection) urlForGetRequest.openConnection();
			conectHttpURL.setRequestMethod("GET"); // phuong thuc gui yeu cau la get
			int responseCode = conectHttpURL.getResponseCode(); // Lay duoc cai trang thai cua requet
			if (responseCode == HttpURLConnection.HTTP_OK) { // ket noi thanh cong
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
				this.sid = (String) ((JSONObject) jsonObject.get("data")).get("sid"); // lay token gan cho bien sid
				// print chuoi token khi login thanh cong
				System.out.println("sid: " + sid);
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
		DateFormat df = new SimpleDateFormat("yyyy/MM/dd -- HH:mm:ss"); // Khoi tao doi tuong format ngay thang nam cua java
		Date date = new Date(); // Doi tuong ngay
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
		String taskid = ""; // chuoi ket qua sau khi checksum
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
	public void down(int id) throws Exception {
		LinkedList<String> listFileSrc = new LinkedList<>();
		String kieuFile = "";
		String loaiFile = "";
		int id_Config = 0;
		
//		goi ham  noi voi DB
		ConnectionDB connectDB = new ConnectionDB();
		Connection connection = connectDB.loadProps();
		
		PreparedStatement pre_selectConfig;
		ResultSet rs_selectConfig;
		if (connection != null) {
//		ket noi thanh cong thi lay du lieu tu table config de dang nhap vao server
			String sql_selectConfig = "SELECT * FROM table_config WHERE id = '" + id + "'";
			pre_selectConfig = connection.prepareStatement(sql_selectConfig);
			rs_selectConfig = pre_selectConfig.executeQuery();
			if (rs_selectConfig.next()) {
				id_Config = rs_selectConfig.getInt("id");
				urlHttp = rs_selectConfig.getString("url");
				usernameDriver = rs_selectConfig.getString("username");
				passwordDriver = rs_selectConfig.getString("password");
				srcFolderDriver = rs_selectConfig.getString("source_folder");
				desSRCLocal = rs_selectConfig.getString("folder_local");
				kieuFile = rs_selectConfig.getString("file");
				loaiFile = rs_selectConfig.getString("table_name");
			}
			if (sid == null) {
				login(urlHttp, usernameDriver, passwordDriver);
			}
			if (sid != null) {
//				login thanh cong
				listFileSrc = listFiles(kieuFile, loaiFile);
//					duyet mang file de down load tung file con ve
				for (int i = 0; i < listFileSrc.size(); i++) {
//						Truoc khi down can phai check file do da co trong log hay chua 
					String srcFileCheck = checkFile(listFileSrc.get(i), id_Config, connection); // goi ham checkfile de kiem tra
					if (srcFileCheck != null) { // file tren server la file moi hoac file co su thay doi
						String nameFile = srcFileCheck.substring(srcFileCheck.lastIndexOf("/") + 1); // lay ra ten file tren server
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

							File file = new File(desSRCLocal + "\\" + nameFile);
							if (file.exists()) {
								String status = "Download_OK"; // gan status cho file co duoi local
//								cau query updat vao bang log
								String sqlUdateLog = "UPDATE table_log SET date_download = ?, status = ? WHERE name_file = '"
										+ nameFile + "'";
								PreparedStatement prepaUpdateLog = connection.prepareStatement(sqlUdateLog);
								prepaUpdateLog.setString(1, getTime());
								prepaUpdateLog.setString(2, status);
								prepaUpdateLog.executeUpdate();

							}
						}
					}
				}
			}
		}
		sid = null; 
		connection.close(); // dong ket noi DB
	}

//	Kiem tra file tren server voi duoi local
	public String checkFile(String pathNameFileSrc, int id_Config, Connection connection) throws Exception {
		String srcFileCheck = "";
		String status = "Server"; // set status nhung file tren local chua duoc down
		String checkMD5 = checkSumMD5FileSrc(pathNameFileSrc); // goi ham checksum file server o tren
		String nameFileLog = pathNameFileSrc.substring(pathNameFileSrc.lastIndexOf("/") + 1); // cat namfileSRC thanh namfile
//	Cau sql selec bang log theo name file de so sanh
		String sqlSelectLog = "SELECT * FROM table_log WHERE name_file = '" + nameFileLog + "' ";
		PreparedStatement pre_Log = connection.prepareStatement(sqlSelectLog);
		ResultSet rsSelect_log = pre_Log.executeQuery();
// Neu da ton tai filename tong log
		if (rsSelect_log.next()) { 
//		tien hanh so sanh md5 
			if (rsSelect_log.getString("md5").equals(checkMD5)) {
				srcFileCheck = null; 
			} else if (!rsSelect_log.getString("md5").equals(checkMD5)) { // neu cung ten ma khac nhau cai MD5
				srcFileCheck = pathNameFileSrc;
//				Cau query de cap nhat vao cot table log
				String updateLog = "UPDATE table_log SET md5 = '" + checkMD5 + "', status = '" + status
						+ "' WHERE name_file = '" + nameFileLog + "'";
				PreparedStatement preUpdateLog = connection.prepareStatement(updateLog);
				preUpdateLog.executeUpdate();
			}
//	La file moi chua ton tai trong log
		} else { 
			srcFileCheck = pathNameFileSrc;
//			Cau query them row vao bang Table_Log
			String insertFileLog = "INSERT INTO table_log(source_folder, name_file, status, md5, config_id) VALUES (?, ?, ?, ?, ?)";
			PreparedStatement preIsFileLog = connection.prepareStatement(insertFileLog);
			preIsFileLog.setString(1, urlHttp + srcFolderDriver);
			preIsFileLog.setString(2, nameFileLog);
			preIsFileLog.setString(3, status);
			preIsFileLog.setString(4, checkMD5);
			preIsFileLog.setInt(5, id_Config);
			preIsFileLog.execute();
		}
		return srcFileCheck; // xuat ra duong dan den file do tren server
	}


	public void down() throws Exception {
		int i = 1;
		while (i < 5) {
			down(i);
			sid = null;
			i++;
		}
	}


}
