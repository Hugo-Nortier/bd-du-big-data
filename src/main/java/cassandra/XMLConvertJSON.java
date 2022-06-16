package cassandra;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;

import org.json.*;

public class XMLConvertJSON {

	public static void main(String[] args) throws Exception {
		File fout = new File("invoice.json");
		FileOutputStream fos = new FileOutputStream(fout);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		BufferedReader br = new BufferedReader(new FileReader("Invoice.xml"));
		try {

			String st = "";
			String line = br.readLine();

			while (line != null) {
				System.out.println(line);
				st += line;
				line = br.readLine();
			}
			JSONObject json = XML.toJSONObject(st);
			String jsonString = json.toString();
			bw.write(jsonString);

		} catch (JSONException e) {
			// TODO: handle exception
			System.out.println(e.toString());
		}

		finally {
			bw.close();
			br.close();
		}

	}
}
