package connectionDB;

import java.io.IOException;
import java.util.Scanner;

import download.Download;
import loadStaging.LoadFile;

public class Run {
	public static void main(String[] args) throws Exception {
		Download download = new Download();
		LoadFile loadStaging = new LoadFile();

		if (args != null) {
			if (args.length > 0 && args[0].equals("download")) {
				if (args.length == 1)
//					Chay cac dong config
					download.down();
				if (args.length == 2) {
//					Chay tung dong
					download.down(Integer.parseInt(args[1]));
				}
			}
			if (args.length > 0 && args[0].equals("load")) {
				loadStaging.run();
			}
		}

	}

}
