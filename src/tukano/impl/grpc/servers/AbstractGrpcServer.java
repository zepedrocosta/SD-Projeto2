package tukano.impl.grpc.servers;

import java.security.KeyStore;
import java.util.logging.Logger;
import io.grpc.Server;
import tukano.impl.discovery.Discovery;
import tukano.impl.java.servers.AbstractServer;
import utils.IP;
import java.io.FileInputStream;
import javax.net.ssl.KeyManagerFactory;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.netty.handler.ssl.SslContextBuilder;

public class AbstractGrpcServer extends AbstractServer {
	private static final String SERVER_BASE_URI = "grpc://%s:%s%s";

	private static final String GRPC_CTX = "/grpc";

	private final String service;

	private final int port;

	protected Server server;
	AbstractGrpcStub stub;

	protected AbstractGrpcServer(Logger log, String service, int port, AbstractGrpcStub stub) throws Exception {
		super(log, service, String.format(SERVER_BASE_URI, IP.hostname(), port, GRPC_CTX));
		//this.server = ServerBuilder.forPort(port).addService(stub).build();
		this.service = service;
		this.port = port;
		this.stub = stub;
		setup();
	}

	protected void start() throws Exception {
		/*Discovery.getInstance().announce(service, super.serverURI);
		
		Log.info(String.format("%s gRPC Server ready @ %s\n", service, serverURI));

		server.start();
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			System.err.println("*** shutting down gRPC server since JVM is shutting down");
			server.shutdownNow();
			System.err.println("*** server shut down");
		}));*/

		Discovery.getInstance().announce(service, super.serverURI);

		Log.info(String.format("%s gRPC Server ready @ %s\n", service, serverURI));

		//server.start().awaitTermination();
		server.start();
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			System.err.println("*** shutting down gRPC server since JVM is shutting down");
			server.shutdownNow();
			System.err.println("*** server shut down");
		}));
	}

	private void setup() throws Exception {
		var keyStore = System.getProperty("javax.net.ssl.keyStore");
		var keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");

		var keystore = KeyStore.getInstance(KeyStore.getDefaultType());
		try (var in = new FileInputStream(keyStore)) {
			keystore.load(in, keyStorePassword.toCharArray());
		}

		var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		keyManagerFactory.init(keystore, keyStorePassword.toCharArray());

		var sslContext = GrpcSslContexts.configure(SslContextBuilder.forServer(keyManagerFactory)).build();
		this.server = NettyServerBuilder.forPort(port).addService(stub).sslContext(sslContext).build();
	}
	
}
