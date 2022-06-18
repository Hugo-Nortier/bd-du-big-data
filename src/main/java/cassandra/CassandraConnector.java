package cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.InvalidQueryException;

public class CassandraConnector {

    private Cluster cluster;

    private Session session;

    /**
     *  Connexion à cassandra
     * @param node
     * @param port
     */
    public void connect(String node, Integer port) {
        Builder b = Cluster.builder().addContactPoint(node);
        if (port != null) {
            b.withPort(port);
        }
        cluster = b.build();

        session = cluster.connect();
    }

    public Session getSession() {
        return this.session;
    }

    public void close() {
        session.close();
        cluster.close();
    }

    public static void main(String[] args) {
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);

        // create keyspace
        client.createKeySpace("projetBD", "SimpleStrategy", 1);
        ResultSet result = client.getSession().execute("SELECT * FROM system_schema.keyspaces;");

        List<String> matchedKeyspaces = result.all().stream()
                .filter(r -> r.getString(0).equals("projetBD".toLowerCase())).map(r -> r.getString(0))
                .collect(Collectors.toList());

        // ici on drop les tables, placer les String des tables à supprimer dans tableToDrop
        //dropTables(client, "person","feedback","product","brandbyproduct","vendor","person_hasinterest_tag","person_knows_person","post","post_hascreator_person","post_hastag_tag","orderBD","invoice");

        System.out.println("\n\u001B[36m keyspaces: " + matchedKeyspaces.toString()+"\n\u001B[0m");

        // creation des tables dans la BD si elles n'existent pas déjà
        //createTables(client);

        //deleteRows(client,"product"," asin IN ('B00KCPUHQU','B00KS73CPU')");

        //insertRows(client, "feedback", new String[]{"asin", "PersonId", "feedback"}, new String[]{"LCJTMDF", "55551445256", "5.0, was very cool and fun to play with this"});

        // interrogation des données
        queriesSelect(client);
        System.exit(0);
    }

    public static void dropTables(CassandraConnector client, String... tableToDrop){
        System.out.println("\n\u001B[36m Dans drop tables \n\u001B[0m");

        for (int i = 0; i < tableToDrop.length; i++){
            try{
                client.getSession().execute("DROP TABLE projetBD."+tableToDrop[i]+";");
                System.out.println("Table "+tableToDrop[i]+" a été supprimée");
            } catch (InvalidQueryException exception){
                System.err.println("Table "+tableToDrop[i]+" n'existe pas");
            }
        }
    }

    public static void deleteRows(CassandraConnector client, String table, String conditionWhere){
        System.out.println("\n\u001B[36m Dans delete enregistrement(s) \n\u001B[0m");
        try{
            ResultSet result;
            result = client.getSession().execute("SELECT COUNT(*) FROM projetBD."+table+";");
            System.out.println("Nb d'enregistrements de la table "+table+": "+result.one().getLong(0));
			if(conditionWhere!="")
                result = client.getSession().execute("DELETE FROM projetBD."+table+" WHERE "+conditionWhere+";");
            else
                result = client.getSession().execute("DELETE FROM projetBD."+table+";");
            System.out.println("Suppression s'est exécutée? result.wasApplied()="+result.wasApplied());
            result = client.getSession().execute("SELECT COUNT(*) FROM projetBD."+table+";");
            System.out.println("Nb d'enregistrements de la table après suppression "+table+": "+result.one().getLong(0));
        } catch (InvalidQueryException exception){
            System.err.println("Table "+table+" n'existe pas OU erreur clause where: "+conditionWhere);
        }
    }

    public static void insertRows(CassandraConnector client, String table, String[] columns, String[] values){
        System.out.println("\n\u001B[36m Dans insert enregistrement(s) \n\u001B[0m");
        try{
            ResultSet result;
            result = client.getSession().execute("SELECT COUNT(*) FROM projetBD."+table+";");
            System.out.println("Nb d'enregistrements de la table "+table+": "+result.one().getLong(0));

            String row=" (";
            for(int i=0;i< columns.length-1;i++)
                row+=columns[i]+", ";
            row+=columns[columns.length-1]+") VALUES (";
            for(int i=0;i< values.length-1;i++)
                row+="'"+values[i]+"',";
            row+="'"+values[values.length-1]+"') IF NOT EXISTS";
            System.out.println("INSERT into projetBD."+table+" "+row);
            result = client.getSession().execute("INSERT into projetBD."+table+" "+row);

            System.out.println("Insertion s'est exécutée? result.wasApplied()="+result.wasApplied());
            result = client.getSession().execute("SELECT COUNT(*) FROM projetBD."+table+";");
            System.out.println("Nb d'enregistrements de la table après insertion "+table+": "+result.one().getLong(0));
        } catch (InvalidQueryException exception){
            System.err.println("Table "+table+" n'existe pas");
        }
    }

    public static void queriesSelect(CassandraConnector client) {
        client.query1();
        client.query2();
        client.query3();
        client.query4();
		client.query5();
		client.query6();
		client.query7();
		client.query8();
		//client.query9();
		client.query10();
    }

    // Query 1. For a given customer, find his/her all related data including
    // profile, orders,invoices,feedback, comments, and posts in the last month,
    // return the category in which he/she has bought the largest number of
    // products, and return the tag which he/she has engaged the greatest times in
    // the posts.
    public void query1() {
        System.out.println("\n\u001B[36m Dans Query 1 \u001B[0m");
        String customerid = "8796093023175";
        System.out.println("\n\u001B[32m Customer id : " + customerid+" \n\u001B[0m");

        ResultSet result = this.getSession().execute("SELECT * FROM projetBD.person WHERE id='" + customerid + "'");
        System.out
                .println("firstName | lastName | gender | birthday | createDate | locationIP | browserUsed | place");
        this.printPeople(result);

        System.out.println("\n\u001B[32m Customer orders :\n\u001B[0m");
        result = this.getSession().execute("SELECT * FROM projetBD.orderBD WHERE personid='" + customerid
                + "' and orderdate > '2022-06-01' and orderdate < '2022-07-01' ALLOW FILTERING");
        this.printOrders(result);

        System.out.println("\n\u001B[32m Customer invoices :\n\u001B[0m");
        result = this.getSession().execute("SELECT * FROM projetBD.invoice WHERE personid='" + customerid
                + "' and orderdate > '2022-06-01' and orderdate < '2022-07-01' ALLOW FILTERING");
        this.printOrders(result);

        System.out.println("\n\u001B[32m Customer feedbacks :\n\u001B[0m");
        //result = this.getSession().execute("SELECT * FROM projetBD.feedback WHERE personid='" + customerid + "' ALLOW FILTERING");

        result = this.getSession().execute("SELECT * FROM projetBD.feedback WHERE personid='" + "4184" + "' ALLOW FILTERING");
        this.printFeedbacks(result);

        System.out.println("\n\u001B[32m Customer posts :\n\u001B[0m");
        result = this.getSession()
                .execute("SELECT * FROM projetBD.post_hascreator_person WHERE personid='" + customerid + "' ALLOW FILTERING");
        this.printPostPerson(result);

        // aucun moyen de retrouver des categories?

        System.out.println("\n\u001B[32m Customer top tags :\n\u001B[0m");
        result = this.getSession()
                .execute("SELECT tagid, COUNT(tagid) FROM projetBD.person_hasinterest_tag WHERE personid='" + customerid
                        + "' GROUP BY tagid ALLOW FILTERING");
        result.forEach(r -> {
            System.out.println("tag " + r.getString("tagid") + " used "
                    + r.getLong(1) + " times.");
        });
    }

    // Query 2. For a given product during a given period, find the people who
    // commented or posted on it, and had bought it.
    public void query2() {
        String productid = "B00ALVD8VG";
        String startdate = "2020-01-01";
        String enddate = "2021-01-01";

        System.out.println("\n\u001B[36m Dans Query 2 \u001B[0m");

        System.out.println("\n\u001B[32m Product id : " + productid+" \n\u001B[0m");

        System.out.println("asin | title | price | imgUrl");
        ResultSet result = this.getSession().execute("SELECT * FROM projetBD.product WHERE asin='" + productid + "' ALLOW FILTERING");
        this.printProduct(result);

        System.out.println("\n\u001B[32m Customer that fave feedback :\n\u001B[0m");
        result = this.getSession().execute("SELECT * FROM projetBD.feedback WHERE asin='" + productid + "' ALLOW FILTERING");
        System.out
                .println("firstName | lastName | gender | birthday | createDate | locationIP | browserUsed | place");
        result.forEach(r -> {
            ResultSet resprod = this.getSession()
                    .execute("SELECT * FROM projetBD.person WHERE id='" + r.getString("personid") + "' ALLOW FILTERING");
            this.printPeople(resprod);
        });

        System.out.println("\n\u001B[32m Customer that bought the product :\n\u001B[0m");
        result = this.getSession().execute("SELECT * FROM projetBD.orderBD WHERE orderline CONTAINS'" + productid
                + "' AND orderdate > '" + startdate + "' AND orderdate < '" + enddate + "' ALLOW FILTERING");
        System.out
                .println("firstName | lastName | gender | birthday | createDate | locationIP | browserUsed | place");
        result.forEach(r -> {
            ResultSet resprod = this.getSession()
                    .execute("SELECT * FROM projetBD.person WHERE id='" + r.getString("personid") + "' ALLOW FILTERING");
            this.printPeople(resprod);
        });
    }

    // Query 3. For a given product during a given period, find people who have
    // undertaken activities related to it, e.g., posts, comments, and review, and
    // return sentences from these texts that contain negative sentiments.
    public void query3() {
        String productid = "B001F0J2JO";

        System.out.println("\n\u001B[36m Dans Query 3 \u001B[0m");

        System.out.println("\n\u001B[32m Product id : " + productid+" \n\u001B[0m");

        System.out.println("asin | title | price | imgUrl");
        ResultSet result = this.getSession().execute("SELECT * FROM projetBD.product WHERE asin='" + productid + "' ALLOW FILTERING");
        this.printProduct(result);

        System.out.println("\n\u001B[32m Negative feedback :\n\u001B[0m");

        result = this.getSession().execute("SELECT * FROM projetBD.feedback WHERE asin='" + productid + "' ALLOW FILTERING");
        this.printNegativeFeedbacks(result);
    }

    // Query 4. Find the top-2 persons who spend the highest amount of money in
    // orders. Then for each person, traverse her knows-graph with 3-hop to find the
    // friends, and finally return the common friends of these two persons.
    public void query4() {

        System.out.println("\n\u001B[36m Dans Query 4 \u001B[0m");

        System.out.println("\n\u001B[32m Top 2 people sorted by most spending : \n\u001B[0m");

        ResultSet result = this.getSession().execute("SELECT personid, sum(totalprice) FROM projetBD.orderBD GROUP BY personid");
        System.out.println();
        List<Row> resultAll = result.all();
        System.out.println("Customer " + resultAll.get(0).getString(0) + " spent "
                + resultAll.get(0).getDecimal(1) + " total.");

        System.out.println("Customer " + resultAll.get(1).getString(0) + " spent "
                + resultAll.get(1).getDecimal(1) + " total.");

        this.printProduct(result);

        result = this.getSession().execute("SELECT personid2 FROM projetBD.person_knows_person where personid='"
				+ resultAll.get(0).getString(0) + "' ALLOW FILTERING");

		ResultSet result2 = this.getSession()
				.execute("SELECT personid2 FROM projetBD.person_knows_person where personid='"
						+ resultAll.get(1).getString(0) + "' ALLOW FILTERING");

        System.out.println("\n\u001B[32m Common friends (1 hop): \n\u001B[0m");

        System.out.println(this.findCommonFriends(result.all(), result2.all()).toString());

    }
    
 // Query 5. Given a start customer and a product category, find persons who are
 	// this customer's friends within 3-hop friendships in Knows graph, besides,
 	// they have bought products in the given category. Finally, return feedback
 	// with the 5-rating review of those bought products.
 	public void query5() {

        System.out.println("\n\u001B[36m Dans Query 5 \u001B[0m");

        System.out.println("\n\u001B[32m Pas de possibilité de retrouver des catégories : \n\u001B[0m");
    }

 	// Query 6. Given customer 1 and customer 2, find persons in the shortest path
 	// between them in the subgraph, and return the TOP 3 best sellers from all
 	// these persons' purchases.
 	public void query6() {

        System.out.println("\n\u001B[36m Dans Query 6 \u001B[0m");

        String customer1 = "8796093023175";
 		String customer2 = "13194139536445";
        System.out.println("\n\u001B[32m Customer id #1: " + customer1+" \u001B[0m");
        System.out.println("\u001B[32m Customer id #2: " + customer2+" \n\u001B[0m");

 		ResultSet result = this.getSession()
 				.execute("SELECT personid2 FROM projetBD.person_knows_person where personid='" + customer1
 						+ "' ALLOW FILTERING");

 		ResultSet result2 = this.getSession()
 				.execute("SELECT personid2 FROM projetBD.person_knows_person where personid='" + customer2
 						+ "' ALLOW FILTERING");

 		ArrayList<String> people = this.findCommonFriends(result.all(), result2.all());

        System.out.println("\n\u001B[32m Common friends (1 hop) of customer " + customer1 + " and customer " + customer2 + ":\n\u001B[0m" + people+" \n");

 		LinkedHashMap<String, Integer> topsellers = new LinkedHashMap<>();

 		for (String p : people) {
 			result = this.getSession()
 					.execute("SELECT * FROM projetBD.orderBD WHERE personid='" + p + "' ALLOW FILTERING");
 			result.forEach(r -> {
 				Object[] str = r.getSet("orderline", String.class).toArray();
 				for (int i = 0; i < str.length; i++) {
 					String strr = (String) str[i];
 					if (!topsellers.containsKey(strr))
 						topsellers.put(strr, 1);
 					else
 						topsellers.put(strr, topsellers.get(strr) + 1);
 				}
 			});
 		}
        System.out.println("\n\u001B[32m Top 3 sellers: \n\u001B[0m");

 		LinkedHashMap<String, Integer> sortedsell = this.sortMap(topsellers);
 		List<String> keys = new ArrayList<>(sortedsell.keySet());
 		for (int i = 0; i < 3; i++) {
 			if (i < keys.size())
 				System.out.println("Product " + keys.get(i) + " sold " + sortedsell.get(keys.get(i)) + " times.");
 		}
 	}

 	// Query 7. For the products of a given vendor with declining sales compare to
 	// the former quarter, analyze the reviews for these items to see if there are
 	// any negative sentiments.
 	public void query7() {

        System.out.println("\n\u001B[36m Dans Query 7 \u001B[0m");
 		String brand = "Pirma";
        System.out.println("\n\u001B[32m Brand: " + brand+" \n\u001B[0m");

        System.out.println("\n\u001B[32m Checking negative feedbacks of \"" + brand + "\"'s products: \n\u001B[0m");

 		ResultSet result = this.getSession()
 				.execute("SELECT asin FROM projetBD.brandbyproduct where brand='" + brand + "' ALLOW FILTERING");
 		result.forEach(r -> {
 			System.out.println("Negative feedbacks for product " + r.getString(0));
 			ResultSet result2 = this.getSession()
 					.execute("SELECT * FROM projetBD.feedback WHERE asin='" + r.getString(0) + "' ALLOW FILTERING");
 			this.printNegativeFeedbacks(result2);
 		});
 	}

 	// Query 8. For all the products of a given category during a given year,
 	// compute its total sales amount, and measure its popularity in the social
 	// media.
 	public void query8() {
        System.out.println("\n\u001B[36m Dans Query 8 \u001B[0m");
        System.out.println("\n\u001B[32m Pas de possibilité de retrouver des catégories : \n\u001B[0m");
    }

 	// Query 9. Find top-3 companies who have the largest amount of sales at one
 	// country, for each company, compare the number of the male and female
 	// customers, and return the most recent posts of them.
 	public void query9() {

        System.out.println("\n\u001B[36m Dans Query 9 \u001B[0m");

 		String country = "Italy";
        System.out.println("\n\u001B[32m Country: " + country+" \n\u001B[0m");

 		LinkedHashMap<String, Integer> topsellers = new LinkedHashMap<>();

 		ResultSet result = this.getSession()
 				.execute("SELECT vendor FROM projetBD.vendor where country='" + country + "' ALLOW FILTERING");

 		result.forEach(r -> {
 			String brand = r.getString(0);
 			topsellers.put(brand, 0);
 			ResultSet result2 = this.getSession()
 					.execute("SELECT asin FROM projetBD.brandbyproduct where brand='" + brand + "' ALLOW FILTERING");

 			result2.forEach(r2 -> {
 				String product = r2.getString(0);
 				int price = 0;

 				ResultSet result3 = this.getSession()
 						.execute("SELECT price FROM projetBD.product where asin='" + product + "' ALLOW FILTERING");

 				List<Row> rr = result3.all();
 				price = rr.get(0).getInt(0);

 				result3 = this.getSession().execute(
 						"SELECT * FROM projetBD.orderBD WHERE orderline CONTAINS'" + product + "' ALLOW FILTERING");

 				rr = result3.all();
 				for (int i = 0;i<rr.size();i++)
 					topsellers.put(brand, topsellers.get(brand) + price);
 			});
 		});

        System.out.println("\n\u001B[32m Top 3 selling companies in "+country+": \n\u001B[0m");

 		LinkedHashMap<String, Integer> sortedsell = this.sortMap(topsellers);
 		List<String> keys = new ArrayList<>(sortedsell.keySet());
 		for (int i = 0; i < 3; i++) {
 			if (i < keys.size())
 				System.out.println("Product " + keys.get(i) + " sold " + sortedsell.get(keys.get(i)) + " times.");
 		}
 	}

 	// Query 10. Find the top-10 most active people by aggregating the posts during
 	// the last year, then calculate their RFM (Recency, Frequency, Monetary) value
 	// in the same period, and return their recent reviews and tags of interest.
 	public void query10() {

        System.out.println("\n\u001B[36m Dans Query 10 \u001B[0m");

        System.out.println("\n\u001B[32m Top 10 active people on forums: \n\u001B[0m");

 		ResultSet result = this.getSession()
 				.execute("SELECT personid, count(postid) FROM projetBD.post_hascreator_person GROUP BY personid");
 		
 		List<Row> resultAll = result.all();
 		for(int i = 0;i<10;i++)
 		System.out.println(
 				"Person " + resultAll.get(0).getString(0) + " published " + resultAll.get(i).getInt(1) + " posts.");

 		//RFM????
 	}
    
    private ArrayList<String> findCommonFriends(List<Row> all, List<Row> all2) {
		ArrayList<String> res = new ArrayList<>();
		for (Row r : all) {
			for (Row r2 : all2) {
				if (r.getString(0).equals(r2.getString(0)))
					res.add(r.getString(0));
			}
		}
		return res;
	}

	public LinkedHashMap<String, Integer> sortMap(LinkedHashMap<String, Integer> map) {
		List<Entry<String, Integer>> capitalList = new LinkedList<>(map.entrySet());

		// call the sort() method of Collections
		Collections.sort(capitalList, (l1, l2) -> l2.getValue().compareTo(l1.getValue()));

		// create a new map
		LinkedHashMap<String, Integer> result = new LinkedHashMap<String, Integer>();

		// get entry from list to the map
		for (Entry<String, Integer> entry : capitalList) {
			result.put(entry.getKey(), entry.getValue());
		}

		return result;
	}

    private void printPostPerson(ResultSet result) {
        System.out.println("id | imagefile | creationDate | locationIP | browserUsed | language | content | length");
        result.forEach(r -> {
            ResultSet resprod = this.getSession()
                    .execute("SELECT * FROM projetBD.post WHERE id='" + r.getString("postid") + "'");
            this.printPost(resprod);
        });
    }

    private void printPost(ResultSet result) {
        result.forEach(r -> {
            System.out.println(r.getString("id") + " | " + r.getString("imagefile") + " | "
                    + r.getDate("creationdate").toString() + " | " + r.getString("locationip") + " | "
                    + r.getString("browserused") + " | " + r.getString("language") + " | "
                    + r.getString("content") + " | " + r.getInt("length"));
        });
    }

    private void printProduct(ResultSet result) {
        result.forEach(r -> {
            System.out.println(r.getString("asin") + " | " + r.getString("title") + " | " + r.getInt("price") + " | "
                    + r.getString("imgurl"));
        });
    }

    private void printFeedbacks(ResultSet result) {
        System.out.println("asin | personid | feedback");
        result.forEach(r -> {
            System.out.println(r.getString("asin") + " | " + r.getString("personid") + " | " + r.getString("feedback"));
        });

    }

    private void printNegativeFeedbacks(ResultSet result) {
        System.out.println(" personid | feedback");
        result.forEach(r -> {
            String rating = r.getString("feedback").split(",", 2)[0];
            if (rating.equals("'1.0"))
                System.out.println(r.getString("personid") + " | " + r.getString("feedback"));
        });

    }

    public void printPeople(ResultSet result) {
        result.forEach(r -> {
            System.out.println(r.getString("firstname") + " | " + r.getString("lastname") + " | "
                    + r.getString("gender") + " | " + r.getDate("birthday").toString() + " | "
                    + r.getDate("createDate").toString() + " | " + r.getString("locationip") + " | "
                    + r.getString("browserused") + " | " + r.getInt("place"));
        });
    }

    public void printOrders(ResultSet result) {

        System.out.println("OrderId | PersonId | OrderDate | TotalPrice | OrderLine ");
        result.forEach(r -> {
            System.out.println(r.getString("orderid") + " | " + r.getString("personid") + " | "
                    + r.getDate("orderdate").toString() + " | " + r.getDecimal("totalprice") + " | "
                    + Arrays.toString(r.getSet("orderline", String.class).toArray()));
        });
    }

    public static void createTables(CassandraConnector client) {
        System.out.println("\n\u001B[36m Dans create tables \n\u001B[0m");

        // create table person
        ArrayList<String> variables = new ArrayList<>();
        variables.add("id text PRIMARY KEY");
        variables.add("firstName text");
        variables.add("lastName text");
        variables.add("gender text");
        variables.add("birthday date");
        variables.add("createDate date");
        variables.add("locationIP text");
        variables.add("browserUsed text");
        variables.add("place int");
        client.createTable("projetBD", "person", variables);
        client.showTable("projetBD", "person");
        System.out.println("Table 'person' a été créée");

        // create table feedback
        variables = new ArrayList<>();
        variables.add("asin text PRIMARY KEY");
        variables.add("PersonId text");
        variables.add("feedback text");
        client.createTable("projetBD", "feedback", variables);
        client.showTable("projetBD", "feedback");
        System.out.println("Table 'feedback' a été créée");

        // create table product
        variables = new ArrayList<>();
        variables.add("asin text PRIMARY KEY");
        variables.add("title text");
        variables.add("price int");
        variables.add("imgUrl text");
        client.createTable("projetBD", "product", variables);
        client.showTable("projetBD", "product");
        System.out.println("Table 'product' a été créée");

        // create table brandbyproduct
        variables = new ArrayList<>();
        variables.add("brand text");
        variables.add("asin text");
        variables.add("PRIMARY KEY (brand, asin)");
        client.createTable("projetBD", "brandbyproduct", variables);
        client.showTable("projetBD", "brandbyproduct");
        System.out.println("Table 'brandbyproduct' a été créée");

        // create table vendor
        variables = new ArrayList<>();
        variables.add("Vendor text PRIMARY KEY");
        variables.add("Country text");
        variables.add("Industry text");
        client.createTable("projetBD", "vendor", variables);
        client.showTable("projetBD", "vendor");
        System.out.println("Table 'vendor' a été créée");

        // create table person_hasinterest_tag
        variables = new ArrayList<>();
        variables.add("Personid text");
        variables.add("Tagid text");
        variables.add("PRIMARY KEY (Personid, Tagid)");
        client.createTable("projetBD", "person_hasinterest_tag", variables);
        client.showTable("projetBD", "person_hasinterest_tag");
        System.out.println("Table 'person_hasinterest_tag' a été créée");

        // create table person_knows_person
        variables = new ArrayList<>();
        variables.add("Personid text");
        variables.add("Personid2 text");
        variables.add("CreationDate date");
        variables.add("PRIMARY KEY (Personid, Personid2)");
        client.createTable("projetBD", "person_knows_person", variables);
        client.showTable("projetBD", "person_knows_person");
        System.out.println("Table 'person_knows_person' a été créée");

        // create table post
        variables = new ArrayList<>();
        variables.add("id text PRIMARY KEY");
        variables.add("imageFile text");
        variables.add("CreationDate date");
        variables.add("locationIP text");
        variables.add("browserUsed text");
        variables.add("language text");
        variables.add("content text");
        variables.add("length int");
        client.createTable("projetBD", "post", variables);
        client.showTable("projetBD", "post");
        System.out.println("Table 'post' a été créée");

        // create table post_hascreator_person
        variables = new ArrayList<>();
        variables.add("Postid text");
        variables.add("Personid text");
        variables.add("PRIMARY KEY (Postid, Personid)");
        client.createTable("projetBD", "post_hascreator_person", variables);
        client.showTable("projetBD", "post_hascreator_person");
        System.out.println("Table 'post_hascreator_person' a été créée");

        // create table post_hastag_tag
        variables = new ArrayList<>();
        variables.add("Postid text");
        variables.add("Tagid text");
        variables.add("PRIMARY KEY (Postid, Tagid)");
        client.createTable("projetBD", "post_hastag_tag", variables);
        client.showTable("projetBD", "post_hastag_tag");
        System.out.println("Table 'post_hastag_tag' a été créée");

        // create table order
        variables = new ArrayList<>();
        variables.add("OrderId text");
        variables.add("PersonId text");
        variables.add("OrderDate date");
        variables.add("TotalPrice decimal");
        variables.add("OrderLine set<text>");
        variables.add("PRIMARY KEY (PersonId,OrderId)");
        client.createTable("projetBD", "orderBD", variables);
        client.showTable("projetBD", "orderBD");
        System.out.println("Table 'order' a été créée");

        // create table invoice
        variables = new ArrayList<>();
        variables.add("OrderId text PRIMARY KEY");
        variables.add("PersonId text");
        variables.add("OrderDate date");
        variables.add("TotalPrice decimal");
        variables.add("OrderLine set<text>");
        client.createTable("projetBD", "invoice", variables);
        client.showTable("projetBD", "invoice");
        System.out.println("Table 'invoice' a été créée");
    }

    private void showTable(String keyspace, String table) {
        ResultSet result = this.getSession().execute("SELECT * FROM " + keyspace + "." + table);
        List<String> columnNames = result.getColumnDefinitions().asList().stream().map(cl -> cl.getName())
                .collect(Collectors.toList());
        System.out.println(table + ": " + columnNames.toString());
    }

    public void createKeySpace(String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb = new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ").append(keyspaceName)
                .append(" WITH replication = {").append("'class':'").append(replicationStrategy)
                .append("','replication_factor':").append(replicationFactor).append("};");

        String query = sb.toString();
        session.execute(query);
    }

    public void createTable(String keyspace, String tablename, ArrayList<String> elements) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(keyspace).append(".")
                .append(tablename).append("(");
        for (int i = 0; i < elements.size(); i++) {
            sb.append(elements.get(i));
            if (i != elements.size() - 1)
                sb.append(",");
        }
        sb.append(");");

        String query = sb.toString();
        session.execute(query);
    }
}
