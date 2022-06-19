package cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
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
     * Connexion à cassandra
     *
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

        switch (args[0]) {
            case "create":
                // create keyspace
                client.createKeySpace("projetBD", "SimpleStrategy", 1);
                ResultSet result = client.getSession().execute("SELECT * FROM system_schema.keyspaces;");

                List<String> matchedKeyspaces = result.all().stream()
                        .filter(r -> r.getString(0).equals("projetBD".toLowerCase())).map(r -> r.getString(0))
                        .collect(Collectors.toList());
                System.out.println("\n\u001B[36m keyspaces: " + matchedKeyspaces.toString() + "\n\u001B[0m");
                createTables(client);
                break;
            case "drop":
                // ici on drop les tables, placer les String des tables à supprimer dans tableToDrop
                //dropTables(client, "post_hascreator_person");
                dropTables(client, "person", "feedback", "product", "brandbyproduct", "vendor", "person_hasinterest_tag", "person_knows_person", "post", "post_hascreator_person", "post_hastag_tag", "orderBD", "invoice");
                break;
            case "delete":
                deleteRows(client, "product", " asin IN ('B00KCPUHQU','B00KS73CPU')");
                break;
            case "insert":
                insertRows(client, "feedback", new String[]{"asin", "PersonId", "feedback"}, new String[]{"LCJTMDF", "55551445256", "5.0, was very cool and fun to play with this"});
                break;
            case "update":
                updateRows(client,"feedback","feedback='3.0, delivery late...I waited 1 month'","asin='B004EBLC76'");
                break;
            case "query":
                queriesSelect(client,args[1]);
                break;
            default:
                break;
        }
        //close la connexion à cassandra
        client.close();
        System.exit(0);
    }

    public static void dropTables(CassandraConnector client, String... tableToDrop) {
        System.out.println("\n\u001B[36m Dans drop tables \n\u001B[0m");

        for (int i = 0; i < tableToDrop.length; i++) {
            try {
                client.getSession().execute("DROP TABLE projetBD." + tableToDrop[i] + ";");
                System.out.println("Table " + tableToDrop[i] + " a été supprimée");
            } catch (InvalidQueryException exception) {
                System.err.println("Table " + tableToDrop[i] + " n'existe pas");
            }
        }
    }

    public static void deleteRows(CassandraConnector client, String table, String conditionWhere) {
        System.out.println("\n\u001B[36m Dans delete enregistrement(s) \n\u001B[0m");
        try {
            ResultSet result;
            result = client.getSession().execute("SELECT COUNT(*) FROM projetBD." + table + ";");
            System.out.println("Nb d'enregistrements de la table " + table + ": " + result.one().getLong(0));
            if (conditionWhere != ""){
                System.out.println("DELETE FROM projetBD." + table + " WHERE " + conditionWhere + ";");
                result = client.getSession().execute("DELETE FROM projetBD." + table + " WHERE " + conditionWhere + ";");
            }
            else{
                result = client.getSession().execute("DELETE FROM projetBD." + table + ";");
                System.out.println("DELETE FROM projetBD." + table + ";");
            }
            System.out.println("Suppression s'est exécutée? result.wasApplied()=" + result.wasApplied());
            result = client.getSession().execute("SELECT COUNT(*) FROM projetBD." + table + ";");
            System.out.println("Nb d'enregistrements de la table après suppression " + table + ": " + result.one().getLong(0));
        } catch (InvalidQueryException exception) {
            System.err.println("Table " + table + " n'existe pas OU erreur clause where: " + conditionWhere);
        }
    }

    public static void updateRows(CassandraConnector client, String table, String set, String conditionWhere) {
        System.out.println("\n\u001B[36m Dans update enregistrement(s) \n\u001B[0m");
        try {
            ResultSet result;
            if (conditionWhere == ""){
                System.out.println("pas de condition where "+ conditionWhere );
                return;
            }
            else{
                String row = "UPDATE projetBD." + table + " SET "+set+" WHERE "+conditionWhere+";";
                result = client.getSession().execute(row);
                System.out.println(row);
            }
            System.out.println("Modification s'est exécutée? result.wasApplied()=" + result.wasApplied());
        } catch (InvalidQueryException exception) {
            System.err.println(exception);
            System.err.println("Table " + table + " n'existe pas OU erreur clause where: " + conditionWhere);
        }
    }

    public static void insertRows(CassandraConnector client, String table, String[] columns, String[] values) {
        System.out.println("\n\u001B[36m Dans insert enregistrement(s) \n\u001B[0m");
        try {
            ResultSet result;
            result = client.getSession().execute("SELECT COUNT(*) FROM projetBD." + table + ";");
            System.out.println("Nb d'enregistrements de la table " + table + ": " + result.one().getLong(0));

            String row = " (";
            for (int i = 0; i < columns.length - 1; i++)
                row += columns[i] + ", ";
            row += columns[columns.length - 1] + ") VALUES (";
            for (int i = 0; i < values.length - 1; i++)
                row += "'" + values[i] + "',";
            row += "'" + values[values.length - 1] + "') IF NOT EXISTS";
            System.out.println("INSERT into projetBD." + table + " " + row);
            result = client.getSession().execute("INSERT into projetBD." + table + " " + row);

            System.out.println("Insertion s'est exécutée? result.wasApplied()=" + result.wasApplied());
            result = client.getSession().execute("SELECT COUNT(*) FROM projetBD." + table + ";");
            System.out.println("Nb d'enregistrements de la table après insertion " + table + ": " + result.one().getLong(0));
        } catch (InvalidQueryException exception) {
            System.err.println("Table " + table + " n'existe pas");
        }
    }

    public static void queriesSelect(CassandraConnector client, String arg) {
        switch (arg) {
            case "1":
                client.query1();
                break;
            case "2":
                client.query2();
                break;
            case "3":
                client.query3();
                break;
            case "4":
                client.query4();
                break;
            case "5":
                client.query5();
                break;
            case "6":
                client.query6();
                break;
            case "7":
                client.query7();
                break;
            case "8":
                client.query8();
                break;
            case "9":
                client.query9();
                break;
            case "10":
                client.query10();
                break;
            default :
                break;
        }
    }

    // Query 1. For a given customer, find his/her all related data including
    // profile, orders,invoices,feedback, comments, and posts in the last month,
    // return the category in which he/she has bought the largest number of
    // products, and return the tag which he/she has engaged the greatest times in
    // the posts.
    public void query1() {
        System.out.println("\n\u001B[36m Dans Query 1 \u001B[0m");
        String customerid = "8796093023175";
        System.out.println("\n\u001B[32m Customer id : " + customerid + " \n\u001B[0m");

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

        System.out.println("\n\u001B[32m Product id : " + productid + " \n\u001B[0m");

        System.out.println("asin | title | price | imgUrl");
        ResultSet result = this.getSession().execute("SELECT * FROM projetBD.product WHERE asin='" + productid + "' ALLOW FILTERING");
        this.printProduct(result);

        System.out.println("\n\u001B[32m Customer that gave feedback :\n\u001B[0m");
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

        System.out.println("\n\u001B[32m Product id : " + productid + " \n\u001B[0m");

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
                + resultAll.get(0).getDecimal(1) + "$ total.");

        System.out.println("Customer " + resultAll.get(1).getString(0) + " spent "
                + resultAll.get(1).getDecimal(1) + "$ total.");

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

        System.out.println("\n\u001B[32m Pas de possibilité de retrouver des catégories :'( \n\u001B[0m");
    }

    // Query 6. Given customer 1 and customer 2, find persons in the shortest path
    // between them in the subgraph, and return the TOP 3 best sellers from all
    // these persons' purchases.
    public void query6() {

        System.out.println("\n\u001B[36m Dans Query 6 \u001B[0m");

        String customer1 = "24189255819365";
        //String customer2 = "17592186045481";
        String customer2 = "2199023259397";

        System.out.println("\n\u001B[32m Customer id #1: " + customer1 + " \u001B[0m");
        System.out.println("\u001B[32m Customer id #2: " + customer2 + " \n\u001B[0m");

        ResultSet result = this.getSession()
                .execute("SELECT personid2 FROM projetBD.person_knows_person where personid='" + customer1
                        + "' ALLOW FILTERING");

        ResultSet result2 = this.getSession()
                .execute("SELECT personid2 FROM projetBD.person_knows_person where personid='" + customer2
                        + "' ALLOW FILTERING");

        ArrayList<String> people = this.findCommonFriends(result.all(), result2.all());

        System.out.println("\n\u001B[32m Common friends (1 hop) of customer " + customer1 + " and customer " + customer2 + ":\n\u001B[0m" + people + " \n");

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
        String brand = "SCOTT_Sports";
        System.out.println("\n\u001B[32m Brand: " + brand + " \n\u001B[0m");

        System.out.println("\n\u001B[32m Checking negative feedbacks of \"" + brand + "\"'s products: \n\u001B[0m");

        ResultSet result = this.getSession()
                .execute("SELECT asin FROM projetBD.brandbyproduct where brand='" + brand + "' ALLOW FILTERING");
        result.forEach(r -> {
            ResultSet result2 = this.getSession()
                    .execute("SELECT * FROM projetBD.feedback WHERE asin='" + r.getString(0) + "' ALLOW FILTERING");
            this.printNegativeFeedbacksForProduct(result2, r.getString(0));
        });
    }

    // Query 8. For all the products of a given category during a given year,
    // compute its total sales amount, and measure its popularity in the social
    // media.
    public void query8() {
        System.out.println("\n\u001B[36m Dans Query 8 \u001B[0m");
        System.out.println("\n\u001B[32m Pas de possibilité de retrouver des catégories :'( \n\u001B[0m");
    }

    // Query 9. Find top-3 companies who have the largest amount of sales at one
    // country, for each company, compare the number of the male and female
    // customers, and return the most recent posts of them.
    public void query9() {

        System.out.println("\n\u001B[36m Dans Query 9 \u001B[0m");

        String country = "Slovenia";
        System.out.println("\n\u001B[32m Country: " + country + " \n\u001B[0m");

        LinkedHashMap<String, Integer> topsellers = new LinkedHashMap<>();
        LinkedHashMap<String, Double> maleFemale = new LinkedHashMap<>();

        ResultSet result = this.getSession()
                .execute("SELECT vendor FROM projetBD.vendor where country='" + country + "' ALLOW FILTERING");

        result.forEach(r -> {
            String brand = r.getString(0);
            System.out.println("\n\u001B[32m Researching brand: " + brand + "\n\u001B[0m");

            topsellers.put(brand, 0);
            AtomicInteger female = new AtomicInteger();
            AtomicInteger male = new AtomicInteger();
            ResultSet result2 = this.getSession()
                    .execute("SELECT asin FROM projetBD.brandbyproduct where brand='" + brand + "' ALLOW FILTERING");

            result2.forEach(r2 -> {
                String product = r2.getString(0);
                System.out.println("product id: " + r2.getString(0));
                int price = 0;

                ResultSet result3 = this.getSession()
                        .execute("SELECT price FROM projetBD.product where asin='" + product + "' ALLOW FILTERING");

                List<Row> rr = result3.all();
                price = rr.get(0).getInt(0);

                result3 = this.getSession().execute(
                        "SELECT * FROM projetBD.orderBD WHERE orderline CONTAINS'" + product + "' ALLOW FILTERING");

                rr = result3.all();
                for (int i = 0; i < rr.size(); i++) {
                    topsellers.put(brand, topsellers.get(brand) + price);
                    result3 = this.getSession().execute(
                            "SELECT gender FROM projetBD.person WHERE id ='" + rr.get(i).getString("personId") + "' ALLOW FILTERING");
                    List<Row> rr3 = result3.all();
                    for (Row r3 : rr3) {
                        if (r3.getString(0).equals("male")) male.getAndIncrement();
                        else female.getAndIncrement();
                    }
                }
            });
            double malePlusFemale = male.get() + female.get();
            double percent = male.get() / malePlusFemale;
            percent = percent * 100;

            maleFemale.put(brand, percent);
        });


        System.out.println("\n\u001B[32m Top selling companies in " + country + ": \n\u001B[0m");

        LinkedHashMap<String, Integer> sortedsell = this.sortMap(topsellers);
        List<String> keys = new ArrayList<>(sortedsell.keySet());
        for (int i = 0; i < 3; i++) {
            if (i < keys.size()) {
                double malePerCent = maleFemale.get(keys.get(i));
                System.out.println("Company " + keys.get(i) + " sold " + sortedsell.get(keys.get(i)) + " $.");
                System.out.println("they had a ratio of " + malePerCent + "% male and " + (100 - malePerCent) + "% female customers");
            }
        }
    }

    // Query 10. Find the top-10 most active people by aggregating the posts during
    // the last year, then calculate their RFM (Recency, Frequency, Monetary) value
    // in the same period, and return their recent reviews and tags of interest.
    public void query10() {
        LinkedHashMap<String, Integer> gensAvecLeurPost = new LinkedHashMap<>();

        System.out.println("\n\u001B[36m Dans Query 10 \u001B[0m");

        System.out.println("\n\u001B[32m Top 10 active people on forums: \n\u001B[0m");

        ResultSet result = this.getSession()
                .execute("SELECT personid, count(postid) FROM projetBD.post_hascreator_person GROUP BY personid");

        List<Row> resultAll = result.all();
        for (int i = 0; i < resultAll.size(); i++) {
            gensAvecLeurPost.put(resultAll.get(0).getString(0) + i, Math.toIntExact(resultAll.get(i).getLong(1)));
            //System.out.println("Person " + resultAll.get(0).getString(0) + " published " + resultAll.get(i).getLong(1) + " posts.");
        }
        LinkedHashMap<String, Integer> sortedsell = this.sortMap(gensAvecLeurPost);
        List<String> keys = new ArrayList<>(sortedsell.keySet());
        for (int i = 0; i < 10; i++) {
            if (i < keys.size()) {
                System.out.println("Person " + keys.get(i) + " published " + sortedsell.get(keys.get(i)) + " posts.");
            }
        }

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

    private void printNegativeFeedbacksForProduct(ResultSet result, String productName) {
        boolean b = true;
        List<Row> resultAll = result.all();
        for (Row r : resultAll) {
            String rating = r.getString("feedback").split(",", 2)[0];
            if (rating.equals("'1.0")) {
                if (b) {
                    System.out.println("\nNegative feedbacks for product " + productName);
                    System.out.println(" personid | feedback");
                }
                System.out.println(r.getString("personid") + " | " + r.getString("feedback")+"\n");
                b = false;
            } else {
                System.out.println("\u001B[33m No\u001B[0m negative feedbacks for product " + productName);
            }
        }
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
        variables.add("PRIMARY KEY (Personid, Postid)");
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
