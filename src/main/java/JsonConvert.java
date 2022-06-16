import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class JsonConvert {
	public static void main(String[] args) throws Exception {
		File fout = new File("order.csv");
		FileOutputStream fos = new FileOutputStream(fout);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		BufferedReader br = new BufferedReader(new FileReader("Order.json"));
		try {
			String line = br.readLine();

			while (line != null) {
				JSONObject jo = (JSONObject) new JSONParser().parse(line);
				JSONArray ja = (JSONArray) jo.get("Orderline");

				Iterator itr2 = ja.iterator();
				String orderline = "{";
				while (itr2.hasNext()) {
					JSONObject jjo = (JSONObject) itr2.next();
					orderline += "\"" + jjo.get("asin") + "\",";

				}
				String orderid = (String) jo.get("OrderId");
				String personid = (String) jo.get("PersonId");
				String orderdate = (String) jo.get("OrderDate");
				Double totalprice = (Double) jo.get("TotalPrice");

				orderline = orderline.substring(0, orderline.length() - 1);
				orderline += "}";
				
				bw.write("\"" + orderid + "\"|\"" + personid + "\"|\"" + orderdate + "\"|" + totalprice + "|"
						+ orderline);
				bw.newLine();
				line = br.readLine();
			}
		} finally {
			br.close();
			bw.close();

		}

	}
}
