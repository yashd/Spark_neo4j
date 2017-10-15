

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.neo4j.driver.v1.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.Schema;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.neo4j.driver.v1.Values.parameters;


public class Home {

    static Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic( "neo4j", "4jneo" ) );
    static Session session = driver.session();

    public static void addTrain(Train train){

        session.run( "MERGE (a:Train { name: {train_name},num:{train_num}}) ",
                parameters("train_name",train.getTrain_name(),"train_num",train.getTrain_num()) );
        System.out.println("Inserted a train into the database");

        session.run("MERGE (int_station:Station{name:{station_name},code:{station_code}})",
                parameters("station_name",train.getInt_station_name(),"station_code",train.getInt_station_code()));

        session.run("MERGE (src_station:Station{name:{station_name},code:{station_code}})",
                parameters("station_name",train.getSrc_station_name(),"station_code",train.getSrc_station_code()));

        session.run("MERGE (des_station:Station{name:{station_name},code:{station_code}})",
                parameters("station_name",train.getDes_station_name(),"station_code",train.getDes_station_code()));

        Station src_station=get_src_station(train);
        Station int_station=get_int_station(train);
        Station des_station=get_des_station(train);


        createRelationship(train,src_station,"SOURCE");


        if( !src_station.getStation_code().equals(int_station.getStation_code())) {

            createRelationship(train,int_station,"INTERMEDIATE");


        }
        else{

            System.out.println("Station codes matched:"+src_station.getStation_code()+":"+int_station.getStation_code());
        }


        createRelationship(train, des_station, "DESTINATION");

    }


    public static Station get_src_station(Train train){

        Station src_station=new Station();
        src_station.setStation_code(train.getSrc_station_code());
        src_station.setStation_name(train.getSrc_station_name());

        return src_station;
    }


    public static Station get_des_station(Train train){

        Station des_station=new Station();
        des_station.setStation_code(train.getDes_station_code());
        des_station.setStation_name(train.getDes_station_name());

        return des_station;
    }
    
    public static Station get_int_station(Train train){

        Station int_station=new Station();
        int_station.setStation_code(train.getInt_station_code());
        int_station.setStation_name(train.getInt_station_name());

        return int_station;
    }

    public  static void createRelationship(Train train,Station station,String relation){


        if (relation == "SOURCE" || relation == "INTERMEDIATE"){

            session.run("MATCH (a:Train { name: {train_name},num:{train_num}}) " +
                            " MATCH(int_station:Station{name:{station_name},code:{station_code}}) " +
                            "MERGE (a)<-[:"+relation+"]-(int_station)",
                    parameters("train_name",train.getTrain_name(),"train_num",train.getTrain_num() ,
                            "station_name",station.getStation_name(),"station_code",station.getStation_code()));

        }
        else {
            session.run("MATCH (a:Train { name: {train_name},num:{train_num}}) " +
                            " MATCH(int_station:Station{name:{station_name},code:{station_code}}) " +
                            "MERGE (a)-[:" + relation + "]->(int_station)",
                    parameters("train_name", train.getTrain_name(), "train_num", train.getTrain_num(),
                            "station_name", station.getStation_name(), "station_code", station.getStation_code()));
        }

    }

    public static Train cleandata(Train train){

        train.setTrain_num(train.getTrain_num().replace("'",""));
        System.out.println("Train num changed"+train.getTrain_num());

        return train;

    }

    public static void main(String[] args){

        SparkSession spark = SparkSession.builder().appName("Railway data").master("local").getOrCreate();
        Dataset<Train> df=spark.read().option("header","True").csv("./src/main/resources/train_detail_2.csv").as(Encoders.bean(Train.class));
        List<Train>trains_list=df.collectAsList();
        for (Train train :trains_list){

            train=cleandata(train);
            addTrain(train);
        }
        System.out.println("End of  Program");

    }
}
