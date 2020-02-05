package com.clouder.hbase.client;

import org.springframework.security.kerberos.client.KerberosRestTemplate;

/**
 * Hello world!
 *
 */
public class HbaseRestClient {

    public static void main(String[] args) {
        executeTest();
    }

    public static void executeTest() {
        try {
            KerberosRestTemplate restTemplate = new KerberosRestTemplate("/Users/knarayanan/joe.keytab", "joe_analyst@FIELD.HORTONWORKS.COM");
            System.out.println(restTemplate.getForObject("https://kncdp10.field.hortonworks.com:20550/emp/1", String.class));

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
