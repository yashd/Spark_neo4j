/**
 * Created by dyaswanth on 9/25/2017.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.JavaRDD;
import java.util.*;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.driver.v1.*;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

import static org.neo4j.driver.v1.Values.parameters;

public class FileReader {


    static Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "4jneo"));
    static Session session = driver.session();

    public static void add_db(Book book){

        //session.run( "CREATE (a:Book { title: {title}}) <-[:AUTHOR]-  (b:Author{Author: {Author}})", parameters( "Author", book.getAuthor(), "title", book.gettitle() ) );
        session.run( "MERGE (a:Book { title: {title}})  MERGE (b:Author{Author: {Author}}) MERGE (a)<-[:AUTHOR]-(b)", parameters( "Author", book.getAuthor(), "title", book.gettitle() ) );
        System.out.println("Inserted a record into a database.");

    }


    private static enum RelTypes implements RelationshipType {
        AUTHOR
    }

    public static void main(String[] args) {

        Encoder<Book> bookEncoder = Encoders.bean(Book.class);

        System.out.println("First dataset");
        SparkSession spark = SparkSession
                .builder()
                .appName("JSON Reader").config("spark.master", "local")
                .getOrCreate();

        // Dataset<Row> df = spark.read().json("src/main/resources/sample.json");
        //df.show();
        System.out.println("Completed First dataframe");
        Dataset<Book> peopleDf = spark.read().json("src/main/resources/sample.json").as(bookEncoder);
        System.out.println("Second dataframe");
        peopleDf.show();

        //session.run("DROP CONSTRAINT ON (author:Author) ASSERT author.Author IS UNIQUE");
        //session.run("DROP CONSTRAINT ON (book:Book) ASSERT book.title IS UNIQUE");

        for (Book book:
                peopleDf.collectAsList()
             ) {
     add_db(book);
        }
        // session.run( "CREATE (a:Book {Author: {Author}, title: {title}})", parameters( "Author", "Arthur", "title", "Legend" ) );

        StatementResult result = session.run( "MATCH (a:Book) WHERE a.Author = {Author} " + "RETURN a.Author AS Author, a.title AS title",parameters( "Author", "Arthur" ) );
        while ( result.hasNext() )
        {
            Record record = result.next();
            System.out.println( record.get( "title" ).asString() + " Written by " + record.get( "Author" ).asString() );
        }


    }

}
