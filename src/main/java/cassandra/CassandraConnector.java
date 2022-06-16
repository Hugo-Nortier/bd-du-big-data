package cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

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

		//client.getSession().execute("DROP TABLE projetBD.person;");
		System.out.println("keyspaces: " + matchedKeyspaces.toString());

		// creation des tables dans la BD si elles n'existent pas déjà
		createTables(client);
		
		result = client.getSession().execute("SELECT * FROM projetBD.person");
		result.forEach(r->{
			System.out.println(r.getString("firstname")+" "+r.getString("lastname"));
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
		variables.add("OrderId text PRIMARY KEY");
		variables.add("PersonId text");
		variables.add("OrderDate date");
		variables.add("TotalPrice decimal");
		variables.add("OrderLine set<text>");

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
