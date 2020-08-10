package connectionDB;

import java.io.IOException;
import java.util.Scanner;

import download.Download;
import loadStaging.LoadFile;

public class Run {
	public static void main(String[] args) throws Exception {
		Download download = new Download();
		LoadFile loadStaging = new LoadFile();
//		Scanner sc = new Scanner(System.in);
//		System.out.println("Nhap id_config: ");
//		String strID_Config = sc.nextLine();
		
		if (args != null) {
			if (args.length > 0 && args[0] == "download") {
				System.out.println("download");
				// download heest trong config
				download.down();
				if (args.length == 2) {
					// download theo id
				}
			}
			if (args.length > 0 && args[0] == "load") {
				loadStaging.run();
			}
		}

	}

}
