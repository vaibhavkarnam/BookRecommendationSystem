package wc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;

/**
 * The Class PreProcessRawTwitterData.
 */
public class PreProcessRawTwitterData {

	/** The file path. */
	private static String filePath = System.getProperty("user.dir");

	/** The input file path. */
	private static String inputFilePath = filePath + "/input";

	/** The output file path. */
	private static String outputFilePath = filePath + "/output/";

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void main(String args[]) throws IOException {
		File folder = new File(inputFilePath);
		File[] listOfFiles = folder.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			File file = listOfFiles[i];
			if (file.isFile()) {
				// String content = FileUtils.readFileToString(file);
				// System.out.println(content);
				BufferedReader br = new BufferedReader(new FileReader(file));
				try {
					StringBuilder sb = new StringBuilder();
					String line = br.readLine();
					while (line != null) {
						sb.append(line);
						sb.append(",,");
						line = br.readLine();
					}
					String content = sb.toString().replaceAll(",,\\*\\*\\*,,\\*\\*\\*,,", System.lineSeparator());

					String[] contentList = Arrays.copyOfRange(content.split(",,"), 1, content.split(",,").length - 1);
					content = String.join(",,", contentList);

					try (PrintWriter out = new PrintWriter(outputFilePath + file.getName() + ".txt")) {
						out.println(content);
					}

				} finally {
					br.close();
				}
			}

		}

	}
}