package com.rfg.pstparser.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.xml.sax.SAXException;

import com.pff.PSTAttachment;
import com.pff.PSTException;
import com.pff.PSTFile;
import com.pff.PSTFolder;
import com.pff.PSTMessage;

public class Test {

	private Client client;
	private BulkRequestBuilder bulkRequest;

	public static void main(String[] args) {
		new Test("/home/josephyuen/Downloads/parser.pst");
	}

	public Test(String filename) {
		try {
			PSTFile pstFile = new PSTFile(filename);

			// Connect to ES
			// client = TransportClient.builder().build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
			// bulkRequest = client.prepareBulk();

			processFolder(pstFile.getRootFolder());

			// Send to ES
			// if(bulkRequest.numberOfActions() > 0){
			// BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			// if (bulkResponse.hasFailures()) {
			// System.out.println("Error sending bulk");
			// }
			// }
		} catch (Exception err) {
			err.printStackTrace();
		}
	}

	int depth = -1;

	public void processFolder(PSTFolder folder) throws PSTException, java.io.IOException {
		 depth++;
		// the root folder doesn't have a display name
		if (depth > 0) {
			printDepth();
			System.out.println(folder.getDisplayName());
		}

		// go through the folders...
		if (folder.hasSubfolders()) {
			Vector<PSTFolder> childFolders = folder.getSubFolders();
			for (PSTFolder childFolder : childFolders) {
				processFolder(childFolder);
			}
		}

		// and now the emails for this folder
		if (folder.getContentCount() > 0) {
			depth++;
			PSTMessage email = (PSTMessage) folder.getNextChild();
			while (email != null) {
				// String senderEmail = email.getSenderEmailAddress();
				// String senderDomain = senderEmail.substring(senderEmail.indexOf("@") + 1);
				//
				// XContentBuilder xb = XContentFactory.jsonBuilder()
				// .startObject()
				// .startArray("tags")
				// .endArray()
				// .field("subject", email.getSubject())
				// .field("recipient", email.getReceivedByName())
				// .field("creationTime", email.getCreationTime())
				// .field("deliveryTime", email.getMessageDeliveryTime())
				// .field("submitTime", email.getClientSubmitTime())
				// .field("body", email.getBody())
				// .field("bodyHTML", email.getBodyHTML())
				// .startObject("sender")
				// .field("name", email.getSenderName())
				// .field("email", senderEmail)
				// .field("domain", senderDomain)
				// .endObject()
				// .startArray("recipients");
				//
				// int numOfRecipients = email.getNumberOfRecipients();
				// for(int i=0; i<numOfRecipients; i++){
				// PSTRecipient recipient = email.getRecipient(i);
				// String sipEmail = recipient.get5FE5();
				// if(sipEmail.startsWith("sip:"))
				// sipEmail = sipEmail.substring(4);
				// String domain = sipEmail.substring(sipEmail.indexOf("@") + 1);
				// xb
				// .startObject()
				// .field("name",recipient.getDisplayName())
				// .field("email",sipEmail)
				// .field("domain",domain)
				// .endObject();
				// }
				//
				// xb.endArray().endObject();
				// bulkRequest.add(client.prepareIndex("Spectris", "outlook").setSource(xb));

				int numOfAttachments = email.getNumberOfAttachments();
				for (int i = 0; i < numOfAttachments; i++) {
//					System.out.println(i);
					PSTAttachment pa = email.getAttachment(i);
//					FileUtils.copyInputStreamToFile(pa.getFileInputStream(), new File("/home/josephyuen/tmp/" + pa.getDisplayName()));

					AutoDetectParser parser = new AutoDetectParser();
					BodyContentHandler handler = new BodyContentHandler();
					Metadata metadata = new Metadata();
					try (InputStream stream = pa.getFileInputStream()) {
						try {
							parser.parse(stream, handler, metadata);
						} catch (SAXException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (TikaException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						// System.out.println(handler.toString());
//						System.out.println(metadata);
//						System.out.println(metadata.get(Metadata.CONTENT_TYPE));
//						System.out.println(metadata.get(TikaCoreProperties.CREATOR));
					}

					// Tika tika = new Tika();
					// try (InputStream stream = pa.getFileInputStream()) {
					// try {
					// System.out.println( tika.parseToString(stream));
					// } catch (TikaException e) {
					// // TODO Auto-generated catch block
					// e.printStackTrace();
					// }
					// }

				}

				email = (PSTMessage) folder.getNextChild();
			}
			depth--;
		}
		depth--;
	}

	public void printDepth() {
		for (int x = 0; x < depth - 1; x++) {
			System.out.print(" | ");
		}
		System.out.print(" |- ");
	}
}