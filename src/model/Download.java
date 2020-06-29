package model;

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
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Download {
	private String sid = null;
	private String urlHttp, usernameDriver, passwordDriver, srcFolderDriver, desSRCLocal, kieuFile;
	private String connectionURL, userName, passWord, sqlConfig, status;
	private String sqlLog;
	private Connection connection;
	private LinkedList<String> listFileSource;
	private LinkedList<Integer> listSizeFileSource;
//	Tao mang chua file check tu source voi local
	private LinkedList<String> listFileCheck;
	private LinkedList<Integer> listSizeFileCheck;

	public Connection loadProps() throws IOException {
		FileInputStream f = new FileInputStream("src/model/dms_config.properties");
		Properties pros = new Properties();
		pros.load(f);
		this.connectionURL = pros.getProperty("connectionURL");
		this.userName = pros.getProperty("userName");
		this.passWord = pros.getProperty("passWord");
		this.sqlConfig = pros.getProperty("sqlconfig");
		this.sqlLog = pros.getProperty("sqllog");
		try {
			connection = DriverManager.getConnection(connectionURL, userName, passWord);
			String lenhSQL = sqlConfig;
			PreparedStatement pre = connection.prepareStatement(lenhSQL);
			ResultSet resultSet = pre.executeQuery();
			while (resultSet.next()) {
				urlHttp = resultSet.getString("url");
				usernameDriver = resultSet.getString("username");
				passwordDriver = resultSet.getString("password");
				srcFolderDriver = resultSet.getString("source_folder");
				desSRCLocal = resultSet.getString("folder_local");
				kieuFile = resultSet.getString("file");
			}

		} catch (SQLException e) {
			System.out.println("Loi ket noi, kiem tra lai");
			System.exit(0);
			e.printStackTrace();
		}
		return connection;
	}

	private void login() throws Exception {
		URL urlForGetRequest = new URL(urlHttp + "/webapi/auth.cgi?api=SYNO.API.Auth&version=3&method=login&account="
				+ usernameDriver + "&passwd=" + passwordDriver + "&session=FileStation&format=cookie");
		HttpURLConnection conection = (HttpURLConnection) urlForGetRequest.openConnection();
		conection.setRequestMethod("GET");
		int responseCode = conection.getResponseCode();
		if (responseCode == HttpURLConnection.HTTP_OK) {
			BufferedReader in = new BufferedReader(new InputStreamReader(conection.getInputStream()));
			StringBuffer response = new StringBuffer();
			String line = null;
			while ((line = in.readLine()) != null) {
				response.append(line);
			}
			in.close();
			JSONParser parser = new JSONParser();
			JSONObject jsonObject = (JSONObject) parser.parse(response.toString());
			this.sid = (String) ((JSONObject) jsonObject.get("data")).get("sid");
			// print result
			System.out.println("sid: " + sid);
			// GetAndPost.POSTRequest(response.toString());
		} else {
			System.out.println("GET NOT WORKED");
		}
	}

	public LinkedList<String> listFiles() throws Exception {
		if (sid != null) {
//			listFileSource.clear();
//			listSizeFileSource.clear();
			listFileSource = new LinkedList<String>();
			listSizeFileSource = new LinkedList<Integer>();
			URL urlForGetRequest = new URL(
					urlHttp + "/webapi/entry.cgi?api=SYNO.FileStation.List&version=1&method=list&folder_path="
							+ srcFolderDriver + "&additional=size&_sid=" + sid);
			HttpURLConnection conection = (HttpURLConnection) urlForGetRequest.openConnection();
			conection.setRequestMethod("GET");
			int responseCode = conection.getResponseCode();
			if (responseCode == HttpURLConnection.HTTP_OK) {
				BufferedReader in = new BufferedReader(new InputStreamReader(conection.getInputStream()));
				StringBuffer response = new StringBuffer();
				String line = null;
				while ((line = in.readLine()) != null) {
					response.append(line);
				}
				in.close();
				JSONParser parser = new JSONParser();
				JSONObject jsonObject = (JSONObject) parser.parse(response.toString());	
				JSONArray files = (JSONArray) ((JSONObject) jsonObject.get("data")).get("files");
				for (int i = 0; i < files.size(); i++) {
					listFileSource.push((String) ((JSONObject) files.get(i)).get("path"));
					String sizeFile = (String) ((JSONObject) files.get(i)).get("additional").toString();
					String[] chuoi = sizeFile.split(":");
					String[] intSize = chuoi[1].split("}");
					listSizeFileSource.push(Integer.parseInt(intSize[0]));
				}
//				for (int i = 0; i < listFileSource.size(); i++) {
//					
//					System.out.println(listFileSource.get(i) + "\t" + listSizeFileSource.get(i));
//				}
//				return listFileSource;
			} else {
				System.out.println("GET LIST FILES NOT WORKED");
			}
		}
		return listFileSource;
	}

	public void down() throws Exception {
		if (sid != null) {
			File file = new File(desSRCLocal);
			if (file.listFiles().length == 0) {
				status = "Download_OK";
				int sumFile = 1;
				LinkedList<String> lisFile = listFiles();
					for (int i = 0; i < lisFile.size(); i++) {
					String srcNameFile = lisFile.get(i);
					String nameFile = srcNameFile.substring(srcNameFile.lastIndexOf("/") + 1);
					if (nameFile.contains(kieuFile)) {
						URL urlForGetRequest = new URL(urlHttp
								+ "/webapi/entry.cgi?api=SYNO.FileStation.Download&version=1&method=download&mode=open&path="
								+ srcNameFile + "&_sid=" + sid);
						HttpURLConnection conection = (HttpURLConnection) urlForGetRequest.openConnection();
						conection.setRequestMethod("GET");
						int responseCode = conection.getResponseCode();
						if (responseCode == HttpURLConnection.HTTP_OK) {
							InputStream in = new BufferedInputStream((conection.getInputStream()));
							
							BufferedOutputStream out = new BufferedOutputStream(
									new FileOutputStream(desSRCLocal + "\\" + nameFile));
							int readData;
							byte[] buff = new byte[1024];
							while ((readData = in.read(buff)) > -1) {
								out.write(buff, 0, readData);
							}
							LocalDate date = java.time.LocalDate.now();
							String insertLog = sqlLog;
							PreparedStatement pre = connection.prepareStatement(insertLog);
							pre.setString(1, urlHttp + srcFolderDriver);
							pre.setString(2, nameFile);
							pre.setString(3, status);
							pre.setInt(4, listSizeFileSource.get(i));
							pre.setString(5, date.toString());
							pre.execute();
							System.out.println("Download file name: " + nameFile + "\t" + "size: " + listSizeFileSource.get(i) + "KB" + "\t" + status + "\t" + java.time.LocalDate.now());
							in.close();
							out.close();
							sumFile ++;
						}
					}
				}
					SendMailSSL senmail = new SendMailSSL();
					senmail.sendMail("Data warehouse nhóm 12 ca sáng", "Đã download tổng " + sumFile + " file từ source về thư mục " + desSRCLocal + "local");
				System.out.println("Download " + sumFile + " file");
				
			}else if (file.listFiles().length > 0){
				status = "Upload";
				System.out.println("Dang lam............chua hoan thanh");
//				LinkedList<String> lisFile = checkFile();
//				for (int i = 0; i < lisFile.size(); i++) {
//					String srcNameFile = lisFile.get(i);
//					String nameFile = srcNameFile.substring(srcNameFile.lastIndexOf("/") + 1);
//					if (nameFile.contains(kieuFile)) {
//						URL urlForGetRequest = new URL(urlHttp
//								+ "/webapi/entry.cgi?api=SYNO.FileStation.Download&version=1&method=download&mode=open&path="
//								+ srcNameFile + "&_sid=" + sid);
//						HttpURLConnection conection = (HttpURLConnection) urlForGetRequest.openConnection();
//						conection.setRequestMethod("GET");
//						int responseCode = conection.getResponseCode();
//						if (responseCode == HttpURLConnection.HTTP_OK) {
//							InputStream in = new BufferedInputStream((conection.getInputStream()));
////				InputStream in = new BufferedInputStream(new FileInputStream(sFile));
//							BufferedOutputStream out = new BufferedOutputStream(
//									new FileOutputStream(desSRCLocal + "\\" + nameFile));
//							int readData;
//							byte[] buff = new byte[1024];
//							while ((readData = in.read(buff)) > -1) {
//								out.write(buff, 0, readData);
//							}
//							String insertLog = sqlLog;
//							PreparedStatement pre = connection.prepareStatement(insertLog);
//							pre.setString(1, urlHttp + srcFolderDriver);
//							pre.setString(2, nameFile);
//							pre.setString(3, status);
//							pre.setInt(4, nameFile.length());
//							pre.execute();
//							System.out.println("Download file name: " + nameFile + "\t" + "||" + readData
//									+ "\t" + "||" + srcFolderDriver + "\t" + "||");
//							in.close();
//							out.close();
//						}
//					}
//				}
			}
		} else {
			System.out.println("Not login!");
		}
	}

//	public LinkedList<String> checkFile() throws Exception {
//		LinkedList<String> lis = listFiles();
//		LinkedList<String> lisFileDinhDang = new LinkedList<>();
//		LinkedList<Integer> lsfc = new LinkedList<>();
//		listFileCheck = new LinkedList<>();
//		listSizeFileCheck = new LinkedList<>();
//		File file = new File(desSRCLocal);
//		File[] lf = file.listFiles();
//		int index = 0;
//		for (int i = 0; i < lis.size(); i++) {
//			String srcNameFile = lis.get(i);
//			String nameFile = srcNameFile.substring(srcNameFile.lastIndexOf("/") + 1);
//			if (nameFile.contains(kieuFile)) {
//				lisFileDinhDang.add(nameFile);
//				lsfc.add(listSizeFileSource.get(i));
//				index++;
//			}
//		}
//		for (int j = 0; j < lisFileDinhDang.size(); j++) {
//			for (int l = 0; l < lf.length; l++) {
//				if (lisFileDinhDang.get(j).equalsIgnoreCase(lf[l].getName()) && lsfc.get(j)!= lf[l].length()) {
//					listFileCheck.add(lisFileDinhDang.get(j));
//					listSizeFileCheck.add(lsfc.get(l));
//				}else if(!lisFileDinhDang.get(j).equalsIgnoreCase(lf[l].getName())) {
//					listFileCheck.add(lisFileDinhDang.get(j));
//					listSizeFileCheck.add(lsfc.get(l));
//				}
//			}
//		}
//		for (int i = 0; i < listFileCheck.size(); i++) {
//			System.out.println(listFileCheck.get(i)+ "\t" + listSizeFileCheck.get(i));			
//		}
//		return listFileCheck;
//	}
	
	

	public static void main(String[] args) throws Exception {
		Download test = new Download();
		test.loadProps();
		test.login();
//		test.listFiles();
		test.down();
//		test.checkFile();
	}

}
