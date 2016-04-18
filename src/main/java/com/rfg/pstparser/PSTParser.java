package com.rfg.pstparser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pff.PSTAttachment;
import com.pff.PSTException;
import com.pff.PSTFile;
import com.pff.PSTFolder;
import com.pff.PSTMessage;
import com.pff.PSTRecipient;

public class PSTParser {

	private static final Logger logger = LogManager.getLogger(PSTParser.class);
	
	private static final String MAGIC_STR = "/cn=recipients/cn=";

	private Client client;
	private BulkRequestBuilder bulkRequest;
	private String indexName;
	private String typeName;
	private String exchangeDomain;
	private String attachmentPath;
	private String pstFileName;
	private String company;
	private String department;
	private LinkedList<String> folderStack = new LinkedList<>();

	public PSTParser(String indexName, String typeName, String exchangeDomain, String pstFolderPath, String attachmentPath, String company, String department) {
		this.indexName = indexName;
		this.typeName = typeName;
		this.exchangeDomain = exchangeDomain;
		this.attachmentPath = attachmentPath;
		this.company = company;
		this.department = department;
		
		// Connect to ES
		try {
			Settings settings = Settings.settingsBuilder()
					.put("client.transport.ping_timeout", "60s")
					.put("client.transport.nodes_sampler_interval", "60s")
					.build();
			client = TransportClient.builder().settings(settings).build()
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
		} catch (UnknownHostException e) {
			logger.error("Unknown host exception", e);
		}
		
		File[] files = new File(pstFolderPath).listFiles();
		processFolder(files);
	}
	
	public void processFolder(File[] files) {
		try {
		    for (File file : files) {
		        if (file.isDirectory()) {
		        	logger.info("Directory: " + file.getName());
//		        	if(file.getName().equals("20150501") || file.getName().equals("20150515")){
//		        		logger.info("Skipping directory");
//		        		continue;
//		        	}
		            processFolder(file.listFiles()); // Calls same method again.
		        } else {
		            pstFileName = file.getName();
		            logger.info("Processing " + pstFileName);
					PSTFile pstFile = new PSTFile(file.getAbsolutePath());
					
					bulkRequest = client.prepareBulk();
					
					processMailFolder(pstFile.getRootFolder());
					
					// Send to ES
					if (bulkRequest.numberOfActions() > 0) {
						logger.info("Sending " + bulkRequest.numberOfActions() + " item to ElasticSearch");
						BulkResponse bulkResponse = bulkRequest.execute().actionGet();
						if (bulkResponse.hasFailures())
							logger.error(bulkResponse.buildFailureMessage());
					}
		        }
		    }
		} catch (Exception err) {
			logger.error("Main loop exception", err);
		}
	}

	public void processMailFolder(PSTFolder folder) throws PSTException, java.io.IOException {

		String folderName = folder.getDisplayName();
		if (!"".equals(folderName))
			folderStack.add(folderName);
		
		// go through the folders...
		if (folder.hasSubfolders()) {
			Vector<PSTFolder> childFolders = folder.getSubFolders();
			for (PSTFolder childFolder : childFolders) {
				processMailFolder(childFolder);
			}
		}
		
		// and now the emails for this folder
		if (folder.getContentCount() > 0) {
			PSTMessage email = (PSTMessage) folder.getNextChild();
			while (email != null) {
				String emailSignature = "";
				String senderEmail = email.getSenderEmailAddress();
				if (!EmailValidator.getInstance().isValid(senderEmail))
					senderEmail = email.getPidTagRecipientSenderSMTPAddress();
				// Final resort
				if (email.getSenderAddrtype().equals("EX") && !EmailValidator.getInstance().isValid(senderEmail)) {
					senderEmail = email.getSenderEmailAddress().toLowerCase();
					if (senderEmail.indexOf(MAGIC_STR) != -1)
						senderEmail = senderEmail.substring(senderEmail.indexOf(MAGIC_STR) + MAGIC_STR.length()) + "@" + exchangeDomain;
				}
				senderEmail = senderEmail.toLowerCase();
				String senderDomain = senderEmail.substring(senderEmail.indexOf("@") + 1);

				XContentBuilder xb =  XContentFactory.jsonBuilder()
					.startObject()
						.startArray("tags")
						.endArray()
						.startArray("sources")
							.startObject()
								.field("fileName", pstFileName)
								.field("folderPath", printFolderPath())
								.field("company", company)
								.field("department", department)
							.endObject()
						.endArray()
		                .field("subject", email.getSubject())
		                .field("docCreationTime", new Date())
		                .field("creationTime", email.getCreationTime())
		                .field("deliveryTime", email.getMessageDeliveryTime())
		                .field("submitTime", email.getClientSubmitTime())
		                .field("body", email.getBody())
		                .field("bodyHTML", email.getBodyHTML())
		                .startObject("sender")
		                	.field("name", email.getSenderName())
		                	.field("email", senderEmail)
		                	.field("domain", senderDomain)
		                .endObject()
		                .startArray("recipients");
				
				emailSignature += department;
				emailSignature += email.getSubject();
				emailSignature += email.getCreationTime();
				emailSignature += email.getBody();
				emailSignature += senderEmail;
				
				int numOfRecipients = email.getNumberOfRecipients();
				for (int i=0; i<numOfRecipients; i++) {
					PSTRecipient recipient = email.getRecipient(i);
					String type = "";
					switch (recipient.getRecipientType()) {
						case PSTRecipient.MAPI_TO:
							type = "to";
							break;
						case PSTRecipient.MAPI_CC:
							type = "cc";
							break;
						case PSTRecipient.MAPI_BCC:
							type = "bcc";
							break;
					}
					
					String recipientEmail = recipient.getEmailAddress();
					if (!EmailValidator.getInstance().isValid(recipientEmail))
						recipientEmail = recipient.get5FE5();
					if (recipientEmail.startsWith("sip:"))
						recipientEmail = recipientEmail.substring(4);
					if (recipientEmail.length() == 0)
						if (EmailValidator.getInstance().isValid(recipient.getDisplayName()))
							recipientEmail = recipient.getDisplayName();
					// Final resort
					if (recipient.getEmailAddressType().equals("EX") && !EmailValidator.getInstance().isValid(recipientEmail)) {
						recipientEmail = recipient.getEmailAddress().toLowerCase();
						if (recipientEmail.indexOf(MAGIC_STR) != -1)
							recipientEmail = recipientEmail.substring(recipientEmail.indexOf(MAGIC_STR) + MAGIC_STR.length()) + "@" + exchangeDomain;
					}
						
					recipientEmail = recipientEmail.toLowerCase();
					String domain = recipientEmail.substring(recipientEmail.indexOf("@") + 1);
					xb
							.startObject()
		                    	.field("name", recipient.getDisplayName())
		                    	.field("type", type)
		                    	.field("email", recipientEmail)
		                    	.field("domain", domain)
		                    .endObject();
					
					emailSignature += type;
					emailSignature += recipientEmail;
				}
				
				xb
						.endArray()
						.startArray("attachments");
				
				String emailId = getMD5(emailSignature);
				int numOfAttachments = email.getNumberOfAttachments();
				for (int i = 0; i < numOfAttachments; i++) {
					PSTAttachment pa = email.getAttachment(i);
					AutoDetectParser parser = new AutoDetectParser();
					BodyContentHandler handler = new BodyContentHandler(-1);
					Metadata metadata = new Metadata();
					try (InputStream stream = pa.getFileInputStream()) {
						parser.parse(stream, handler, metadata);
						
						String fileName = pa.getFilename();
						if (fileName.length() == 0)
							fileName = getMD5(handler.toString() + pa.getLastModificationTime());
						
						try {
							FileUtils.copyInputStreamToFile(pa.getFileInputStream(), new File(attachmentPath + "/" + indexName + "/" + emailId + "/" + fileName));
						} catch (IOException fuioe) {
							logger.error("Fail to output file: " + attachmentPath + "/" + indexName + "/" + emailId + "/" + fileName);
							logger.error(fuioe.getMessage());
						}
						
						xb
							.startObject()
								.field("name", fileName)
								.field("path", indexName + "/" + emailId + "/" + fileName)
								.field("type", metadata.get(Metadata.CONTENT_TYPE))
								.field("content", handler.toString())
								.field("size", pa.getSize())
							.endObject();
					} catch (Exception e) {
						logger.error("[" + emailId + "] Error generating attachment object: " + e.getMessage());
					}
				}
				xb
						.endArray()
					.endObject();
				
				XContentBuilder paramXB = XContentFactory.jsonBuilder()
				.startObject()
					.field("fileName", pstFileName)
					.field("folderPath", printFolderPath())
					.field("company", company)
					.field("department", department)
				.endObject();
				Map<String, Object> params = new HashMap<>();
				params.put("param", new ObjectMapper().readValue(paramXB.string(), HashMap.class));
				Script script = new Script("if (!ctx._source.sources.contains(param)) {ctx._source.sources += param;}", ScriptService.ScriptType.INLINE, null, params);
				bulkRequest.add(client.prepareUpdate(indexName, typeName, emailId).setScript(script).setUpsert(xb));
				
				email = (PSTMessage) folder.getNextChild();
				
				// Send to ES
				if (bulkRequest.numberOfActions() > 0 && bulkRequest.numberOfActions() % 100 == 0) {
					logger.info("Sending " + bulkRequest.numberOfActions() + " item to ElasticSearch");
					BulkResponse bulkResponse = bulkRequest.execute().actionGet();
					if (bulkResponse.hasFailures())
						logger.error(bulkResponse.buildFailureMessage());
					bulkRequest = client.prepareBulk();
				}
			}
		}
		
		if (folderStack.size() > 0)
			folderStack.removeLast();
	}
	
	public String getMD5(String input) {
		MessageDigest m = null;
		try {
			m = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		m.reset();
		m.update(input.getBytes());
		byte[] digest = m.digest();
		BigInteger bigInt = new BigInteger(1,digest);
		String hashtext = bigInt.toString(16);
		// Now we need to zero pad it if you actually want the full 32 chars.
		while(hashtext.length() < 32){
		  hashtext = "0" + hashtext;
		}
		return hashtext;
	}
	
	public String printFolderPath() {
		String path = "";
		for (String s : folderStack)
			path += s + "/";
		return path.substring(0, path.length() - 1);
	}
	
	public static void main(String[] args) {
		if (args.length != 7) {
			System.out.println("Usage:");
			System.out.println("Arg 0: Index name");
			System.out.println("Arg 1: Type name");
			System.out.println("Arg 2: Exchange domain");
			System.out.println("Arg 3: PST folder path");
			System.out.println("Arg 4: Email attachments output path");
			System.out.println("Arg 5: Company name");
			System.out.println("Arg 6: Department name");
			return;
		}
		
		logger.info("PSTParser started");
//		new PSTParser("spectris", "bksv_china", "spectris.com.cn", "/home/josephyuen/Downloads/2014 Jan to Dec - final count 7812 emails/Messages/Sales employee jan to dec 8741_0001.pst", "/home/josephyuen/tmp", "Avaya", "batch1");
//		new PSTParser("spectris", "bksv_china", "spectris.com.cn", "/home/josephyuen/Downloads/import/", "/home/josephyuen/tmp", "Avaya", "batch1");
		new PSTParser(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
		logger.info("PSTParser ended");
	}
}
