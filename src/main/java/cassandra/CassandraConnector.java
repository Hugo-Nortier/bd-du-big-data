package cassandra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;

public class CassandraConnector {

    private Cluster cluster;

    private Session session;

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

        //client.getSession().execute("DROP TABLE projetBD.orderBD;");
        System.out.println("keyspaces: " + matchedKeyspaces.toString());

        // creation des tables dans la BD si elles n'existent pas déjà
        //createTables(client);

        // interrogation des données
        queriesSelect(client);
        System.exit(0);
    }

    public static void queriesSelect(CassandraConnector client) {
        //client.query1();
        //client.query2();
        //client.query3();
        client.query4();

    }

    // Query 1. For a given customer, find his/her all related data including
    // profile, orders,invoices,feedback, comments, and posts in the last month,
    // return the category in which he/she has bought the largest number of
    // products, and return the tag which he/she has engaged the greatest times in
    // the posts.
    public void query1() {
        String customerid = "8796093023175";

        System.out.println("Query 1");

        System.out.println("Customer id : " + customerid);
        ResultSet result = this.getSession().execute("SELECT * FROM projetBD.person WHERE id='" + customerid + "'");
        System.out
                .println("firstName | lastName | gender | birthday | createDate | locationIP | browserUsed | place");
        this.printPeople(result);

        System.out.println("\nCustomer orders :\n");
        result = this.getSession().execute("SELECT * FROM projetBD.orderBD WHERE personid='" + customerid
                + "' and orderdate > '2022-06-01' and orderdate < '2022-07-01' ALLOW FILTERING");
        this.printOrders(result);

        System.out.println("\nCustomer invoices :\n");
        result = this.getSession().execute("SELECT * FROM projetBD.invoice WHERE personid='" + customerid
                + "' and orderdate > '2022-06-01' and orderdate < '2022-07-01' ALLOW FILTERING");
        this.printOrders(result);

        System.out.println("Customer feedbacks :");
        //result = this.getSession().execute("SELECT * FROM projetBD.feedback WHERE personid='" + customerid + "' ALLOW FILTERING");

        result = this.getSession().execute("SELECT * FROM projetBD.feedback WHERE personid='" + "4184" + "' ALLOW FILTERING");
        this.printFeedbacks(result);

        System.out.println("Customer posts :");
        result = this.getSession()
                .execute("SELECT * FROM projetBD.post_hascreator_person WHERE personid='" + customerid + "' ALLOW FILTERING");
        this.printPostPerson(result);

        // aucun moyen de retrouver des categories?

        System.out.println(" top tags :");
        result = this.getSession()
                .execute("SELECT tagid, COUNT(tagid) FROM projetBD.person_hasinterest_tag WHERE personid='" + customerid
                        + "' GROUP BY tagid ALLOW FILTERING");
        result.forEach(r -> {
            System.out.println("Customer top tag " + r.getString("tagid") + " used "
                    + r.getLong(1) + " times.");
        });

    }

    // Query 2. For a given product during a given period, find the people who
    // commented or posted on it, and had bought it.
    public void query2() {
        String productid = "B00ALVD8VG";
        String startdate = "2020-01-01";
        String enddate = "2021-01-01";

        System.out.println("Query 2");

        System.out.println("Product id : " + productid);

        System.out.println("asin | title | price | imgUrl");
        ResultSet result = this.getSession().execute("SELECT * FROM projetBD.product WHERE asin='" + productid + "' ALLOW FILTERING");
        this.printProduct(result);

        System.out.println("Customers that gave feedback :");
        result = this.getSession().execute("SELECT * FROM projetBD.feedback WHERE asin='" + productid + "' ALLOW FILTERING");
        System.out
                .println("firstName | lastName | gender | birthday | createDate | locationIP | browserUsed | place");
        result.forEach(r -> {
            ResultSet resprod = this.getSession()
                    .execute("SELECT * FROM projetBD.person WHERE id='" + r.getString("personid") + "' ALLOW FILTERING");
            this.printPeople(resprod);
        });

        System.out.println("Customers that bought the product :");
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

        System.out.println("Query 3");

        System.out.println("Product id : " + productid);

        System.out.println("asin | title | price | imgUrl");
        ResultSet result = this.getSession().execute("SELECT * FROM projetBD.product WHERE asin='" + productid + "' ALLOW FILTERING");
        this.printProduct(result);

        System.out.println("Negative feedback :");
        result = this.getSession().execute("SELECT * FROM projetBD.feedback WHERE asin='" + productid + "' ALLOW FILTERING");
        this.printNegativeFeedbacks(result);
    }

    // Query 4. Find the top-2 persons who spend the highest amount of money in
    // orders. Then for each person, traverse her knows-graph with 3-hop to find the
    // friends, and finally return the common friends of these two persons.
    public void query4() {

        System.out.println("Query 4");

        System.out.println("Top 2 people sorted by most spending :");

        ResultSet result = this.getSession().execute("SELECT personid, sum(totalprice) FROM projetBD.orderBD GROUP BY personid");
        System.out.println();
        List<Row> resultAll = result.all();
        System.out.println("Customer " + resultAll.get(0).getString(0) + " spent "
                + resultAll.get(0).getDecimal(1) + " total.");

        System.out.println("Customer " + resultAll.get(1).getString(0) + " spent "
                + resultAll.get(1).getDecimal(1) + " total.");

        this.printProduct(result);

        //graphs pas disponibles sur cassandra

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
            String feedback = r.getString("feedback").split(",", 2)[1];
            //System.out.println("rating"+rating+" feedback "+feedback);
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

        // create table feedback
        variables = new ArrayList<>();
        variables.add("asin text PRIMARY KEY");
        variables.add("PersonId text");
        variables.add("feedback text");
        client.createTable("projetBD", "feedback", variables);
        client.showTable("projetBD", "feedback");

        // create table product
        variables = new ArrayList<>();
        variables.add("asin text PRIMARY KEY");
        variables.add("title text");
        variables.add("price int");
        variables.add("imgUrl text");

        client.createTable("projetBD", "product", variables);
        client.showTable("projetBD", "product");

        // create table brandbyproduct
        variables = new ArrayList<>();
        variables.add("brand text");
        variables.add("asin text");
        variables.add("PRIMARY KEY (brand, asin)");

        client.createTable("projetBD", "brandbyproduct", variables);
        client.showTable("projetBD", "brandbyproduct");

        // create table vendor
        variables = new ArrayList<>();
        variables.add("Vendor text PRIMARY KEY");
        variables.add("Country text");
        variables.add("Industry text");

        client.createTable("projetBD", "vendor", variables);
        client.showTable("projetBD", "vendor");

        // create table person_hasinterest_tag
        variables = new ArrayList<>();
        variables.add("Personid text");
        variables.add("Tagid text");
        variables.add("PRIMARY KEY (Personid, Tagid)");

        client.createTable("projetBD", "person_hasinterest_tag", variables);
        client.showTable("projetBD", "person_hasinterest_tag");

        // create table person_knows_person
        variables = new ArrayList<>();
        variables.add("Personid text");
        variables.add("Personid2 text");
        variables.add("CreationDate date");
        variables.add("PRIMARY KEY (Personid, Personid2)");

        client.createTable("projetBD", "person_knows_person", variables);
        client.showTable("projetBD", "person_knows_person");

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

        // create table post_hascreator_person
        variables = new ArrayList<>();
        variables.add("Postid text");
        variables.add("Personid text");
        variables.add("PRIMARY KEY (Postid, Personid)");

        client.createTable("projetBD", "post_hascreator_person", variables);
        client.showTable("projetBD", "post_hascreator_person");

        // create table post_hastag_tag
        variables = new ArrayList<>();
        variables.add("Postid text");
        variables.add("Tagid text");
        variables.add("PRIMARY KEY (Postid, Tagid)");

        client.createTable("projetBD", "post_hastag_tag", variables);
        client.showTable("projetBD", "post_hastag_tag");

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

        // create table invoice
        variables = new ArrayList<>();
        variables.add("OrderId text PRIMARY KEY");
        variables.add("PersonId text");
        variables.add("OrderDate date");
        variables.add("TotalPrice decimal");
        variables.add("OrderLine set<text>");

        client.createTable("projetBD", "invoice", variables);
        client.showTable("projetBD", "invoice");

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
