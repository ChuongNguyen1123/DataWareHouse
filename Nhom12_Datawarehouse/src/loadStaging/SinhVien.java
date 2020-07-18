package loadStaging ;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import connectionDB.ConnectionDB;

public class SinhVien {
	String stt;
	String maSV;
	String hoLot;
	String ten;
	String ngaySinh;
	String maLop;
	String tenLop;
	String dtLienLac;
	String email;
	String queQuan;
	String ghiChu;
	public SinhVien() {
		super();
	}

	public SinhVien(String stt, String maSV, String hoLot, String ten, String ngaySinh, String maLop, String tenLop,
			String dtLienLac, String email, String queQuan, String ghiChu) {
		super();
		this.stt = stt;
		this.maSV = maSV;
		this.hoLot = hoLot;
		this.ten = ten;
		this.ngaySinh = ngaySinh;
		this.maLop = maLop;
		this.tenLop = tenLop;
		this.dtLienLac = dtLienLac;
		this.email = email;
		this.queQuan = queQuan;
		this.ghiChu = ghiChu;
	}

	public String getStt() {
		return stt;
	}

	public void setStt(String stt) {
		this.stt = stt;
	}

	public String getMaSV() {
		return maSV;
	}

	public void setMaSV(String maSV) {
		this.maSV = maSV;
	}

	public String getHoLot() {
		return hoLot;
	}

	public void setHoLot(String hoLot) {
		this.hoLot = hoLot;
	}

	public String getTen() {
		return ten;
	}

	public void setTen(String ten) {
		this.ten = ten;
	}

	public String getNgaySinh() {
		return ngaySinh;
	}

	public void setNgaySinh(String ngaySinh) {
		this.ngaySinh = ngaySinh;
	}

	public String getMaLop() {
		return maLop;
	}

	public void setMaLop(String maLop) {
		this.maLop = maLop;
	}

	public String getTenLop() {
		return tenLop;
	}

	public void setTenLop(String tenLop) {
		this.tenLop = tenLop;
	}

	public String getDtLienLac() {
		return dtLienLac;
	}

	public void setDtLienLac(String dtLienLac) {
		this.dtLienLac = dtLienLac;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getQueQuan() {
		return queQuan;
	}

	public void setQueQuan(String queQuan) {
		this.queQuan = queQuan;
	}

	public String getGhiChu() {
		return ghiChu;
	}

	public void setGhiChu(String ghiChu) {
		this.ghiChu = ghiChu;
	}

	@Override
	public String toString() {
		return stt + "," + maSV + "," + hoLot + "," + ten + "," + ngaySinh + "," + maLop + "," + tenLop + "," + dtLienLac + "," + email + "," + queQuan + "," + ghiChu;
	}
	public static String[] getConfigInformation1() throws ClassNotFoundException, SQLException, IOException {
		String[] line = new String[14];
		ConnectionDB connect = new ConnectionDB();
	       Connection  connection = connect.loadProps();
		String sql = "SELECT * FROM sinhvien ";
		PreparedStatement ps = connection.prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		while (rs.next()) {
			line[0] = Integer.toString(rs.getInt("id"));
			line[1] = rs.getString("stt");
			line[2] = rs.getString("maSV");
			line[3] = rs.getString("hoLot");
			line[4] = rs.getString("ten");
			line[5] = rs.getString("ngaySinh");
			line[6] = rs.getString("maLop");
			line[7] = rs.getString("tenLop");
			line[8] = rs.getString("dtLienLac");
			line[9] = rs.getString("email");
			line[10] = rs.getString("queQuan");
			line[11] = rs.getString("ghiChu");
		
		
		}
		connection.close();
		return line;
	}
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		String[] line = SinhVien.getConfigInformation1();
		for (int i = 0; i < line.length; i++) {
			System.out.println(line[i]);
			
		
		}
	}
	
}
