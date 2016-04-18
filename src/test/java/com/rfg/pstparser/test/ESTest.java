package com.rfg.pstparser.test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ESTest {
	
	private Client client;
	
	public ESTest() {
		try {
			client = TransportClient.builder().build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void init() {
		try {
			//@formatter:off
			IndexResponse response = client.prepareIndex("twitter", "tweet")
			        .setSource(jsonBuilder()
			                    .startObject()
			                        .field("user", "kimchy")
			                        .field("postDate", new Date())
			                        .field("message", "trying out Elasticsearch")
			                    .endObject()
			                  )
			        .execute()
			        .actionGet();
			//@formatter:on

			client.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void search() {

	}

	public void delete() {
		client.prepareDelete().setIndex("twitter").execute().actionGet();
	}

	public static void main(String[] args) {
		ESTest test = new ESTest();
//		test.init();
		
		test.delete();
	}

}
