package com.clouder.hbase.client;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Hello world!
 *
 */
public class HbaseRestClient2 {

    public static void main(String[] args) {
        executeTest();
    }

    public static void executeTest() {
        try {
            Configuration configuration = HBaseConfiguration.create();
            // FileInputStream fis = new FileInputStream("/Users/knarayanan/Downloads/hbase-conf/hbase-site.xml");
            configuration.addResource(new Path("file:///Users/knarayanan/Downloads/hbase-conf/core-site.xml"));
            configuration.addResource(new Path("file:///Users/knarayanan/Downloads/hbase-conf/hbase-site.xml"));
            configuration.addResource(new Path("file:///Users/knarayanan/Downloads/hbase-conf/hdfs-site.xml"));
            // configuration.addResource("/Users/knarayanan/Downloads/hbase-conf/log4j.properties");
            System.setProperty("java.security.krb5.conf", "/Users/knarayanan/keys/krb5.conf");
            // Enable/disable krb5 debugging
            System.setProperty("sun.security.krb5.debug", "false");

            Map<String, String> values = configuration.getPropsWithPrefix("hbase.zookeeper");
            for (String key : values.keySet()) {
                System.out.println(key + "  " + values.get(key));
            }
            UserGroupInformation.setConfiguration(configuration);

            UserGroupInformation.loginUserFromKeytab("joe_analyst@FIELD.HORTONWORKS.COM", "/Users/knarayanan/joe.keytab");

            Cluster cluster = new Cluster();
            cluster.add("kncdp10.field.hortonworks.com", 20550);
            Client client = new Client(cluster, true);
            RemoteHTable table = new RemoteHTable(client, "emp");

            // Instantiating the Scan class
            Scan scan = new Scan();

            // Scanning the required columns
            scan.addColumn(Bytes.toBytes("person"), Bytes.toBytes("name"));
            scan.addColumn(Bytes.toBytes("person"), Bytes.toBytes("age"));

            // Getting the scan result
            ResultScanner scanner = table.getScanner(scan);

            // Reading values from scan result
            for (Result result = scanner.next(); result != null; result = scanner.next())

                System.out.println("Found row : " + result);
            // closing the scanner
            scanner.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
