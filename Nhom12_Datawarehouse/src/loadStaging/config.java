package loadStaging ;

public class config {
	int id;
	String url;
	String username;
	String password;
	String source_folder;
	String folder_local;
	String file;
	String filetype;
	String staging_table;
	String filed_name;
	int number_cols;
	@Override
	public String toString() {
		return "config [id=" + id + ", url=" + url + ", username=" + username + ", password=" + password
				+ ", source_folder=" + source_folder + ", folder_local=" + folder_local + ", file=" + file
				+ ", filetype=" + filetype + ", staging_table=" + staging_table + ", filed_name=" + filed_name
				+ ", number_cols=" + number_cols + "]";
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getSource_folder() {
		return source_folder;
	}
	public void setSource_folder(String source_folder) {
		this.source_folder = source_folder;
	}
	public String getFolder_local() {
		return folder_local;
	}
	public void setFolder_local(String folder_local) {
		this.folder_local = folder_local;
	}
	public String getFile() {
		return file;
	}
	public void setFile(String file) {
		this.file = file;
	}
	public String getFiletype() {
		return filetype;
	}
	public void setFiletype(String filetype) {
		this.filetype = filetype;
	}
	public String getStaging_table() {
		return staging_table;
	}
	public void setStaging_table(String staging_table) {
		this.staging_table = staging_table;
	}
	public String getFiled_name() {
		return filed_name;
	}
	public void setFiled_name(String filed_name) {
		this.filed_name = filed_name;
	}
	public int getNumber_cols() {
		return number_cols;
	}
	public void setNumber_cols(int number_cols) {
		this.number_cols = number_cols;
	}

}
