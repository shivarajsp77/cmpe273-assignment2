package edu.sjsu.cmpe.library.msg;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;

import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;
import edu.sjsu.cmpe.library.domain.Book;
import edu.sjsu.cmpe.library.domain.Book.Status;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;

public class Listener {
	
	private String apolloUser;
	private String apolloPassword;
	private String apolloHost;
	private int apolloPort;
	private String stompTopic;
	private final LibraryServiceConfiguration configuration;
	private BookRepositoryInterface bookRepository;
	private final Book dummyBook = new Book();

	public Listener(LibraryServiceConfiguration config, BookRepositoryInterface bookRepository) {
		this.configuration = config;
		this.bookRepository = bookRepository;
		apolloUser = configuration.getApolloUser();
		apolloPassword = configuration.getApolloPassword();
		apolloHost = configuration.getApolloHost();
		apolloPort = configuration.getApolloPort();
		stompTopic = configuration.getStompTopicName();
		
	}
	
	public Runnable listener() throws JMSException {
    	long isbn;
    	String bookTitle;
    	String bookCategory;
    	String webURL;
    	Book tempBook = new Book();
    	ArrayList<String> arrivals = new ArrayList<String>();
    	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
    	factory.setBrokerURI("tcp://" + apolloHost + ":" + apolloPort);
    	System.currentTimeMillis();
    	System.out.println("Waiting for messages...");
    	while (true) {
    		//System.out.println(i++);
        	Connection connection = factory.createConnection(apolloUser, apolloPassword);
        	connection.start();
        	Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        	Destination dest = new StompJmsDestination(stompTopic);
        	MessageConsumer consumer = session.createConsumer(dest);
    		while (true) {
    			//System.out.println(j++);
				Message msg = consumer.receive(500);
				if (msg == null)
					break;
				if (msg instanceof TextMessage) {
					String body = ((TextMessage) msg).getText();
					//System.out.println("Received message1 = " + body);
					arrivals.add(body);

				} else {
					System.out.println("Unexpected message type: "
							+ msg.getClass());
				}
				
			}
			connection.close();
			if(!arrivals.isEmpty()) {
			//System.out.println(arrivals);
			for(String arrival:arrivals){
				//System.out.println("couting"+i++);
				isbn = Long.parseLong(arrival.split(":")[0]);
				bookTitle = arrival.split(":")[1].replaceAll("^\"|\"$", "");
				bookCategory = arrival.split(":")[2].replaceAll("^\"|\"$", "");
				webURL = arrival.split(":\"")[3];
				webURL = webURL.substring(0, webURL.length()-1);
				tempBook = bookRepository.getBookByISBN(isbn);
				System.out.println("tempBook is "+tempBook);
				System.out.println("dummyBook is "+dummyBook);
				
				if (tempBook.getIsbn()==0) {
					System.out.println("reachable");
					tempBook.setIsbn(isbn);
					tempBook.setCategory(bookCategory);
					tempBook.setTitle(bookTitle);
					try {
						tempBook.setCoverimage(new URL(webURL));
					} catch (MalformedURLException e) {
						e.printStackTrace();
					}
					bookRepository.addBook(tempBook);
					
				}
				else {
					System.out.println("reachable, changing to available");
					tempBook.setStatus(Status.available);
				}
				
			}
			arrivals.clear();
			
    	}
		}


}
}
