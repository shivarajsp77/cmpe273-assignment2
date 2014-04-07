package edu.sjsu.cmpe.library.msg;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;

import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;

public class Producer {
	private String apolloUser;
	private String apolloPassword;
	private String apolloHost;
	private int apolloPort;
	private String stompQueue;
	private final LibraryServiceConfiguration configuration;
	public Producer(LibraryServiceConfiguration config) {
		this.configuration = config;
		apolloUser = configuration.getApolloUser();
		apolloPassword = configuration.getApolloPassword();
		apolloHost = configuration.getApolloHost();
		apolloPort = configuration.getApolloPort();
		stompQueue = configuration.getStompQueueName();
		
	}
	public void producer(String tempMsg) throws JMSException{
    	System.out.println("checkpint4");
    	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
    	factory.setBrokerURI("tcp://" + apolloHost + ":" + apolloPort);
    	System.out.println(factory.getBrokerURI());

    	Connection connection = factory.createConnection(apolloUser, apolloPassword);
    	connection.start();
    	Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    	Destination dest = new StompJmsDestination(stompQueue);
    	MessageProducer producer = session.createProducer(dest);

    	TextMessage msg = session.createTextMessage(tempMsg);
    	msg.setLongProperty("id", System.currentTimeMillis());
    	System.out.println(msg.getText());
    	producer.send(msg);
    	connection.close();

        }

}
