package loadStaging ;

public class SinhVien {
	int stt;
	int maSV;
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

	public SinhVien(int stt, int maSV, String hoLot, String ten, String ngaySinh, String maLop, String tenLop,
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

	public int getStt() {
		return stt;
	}

	public void setStt(int stt) {
		this.stt = stt;
	}

	public int getMaSV() {
		return maSV;
	}

	public void setMaSV(int maSV) {
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
	
}
