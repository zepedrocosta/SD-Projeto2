package tukano.impl.grpc.servers;

import java.io.IOException;
import java.util.logging.Logger;

import tukano.api.java.Shorts;
import utils.Args;

public class GrpcShortsServer extends AbstractGrpcServer {
public static final int PORT = 14567;
	
	private static Logger Log = Logger.getLogger(GrpcShortsServer.class.getName());

	public GrpcShortsServer() throws Exception {
		super( Log, Shorts.NAME, PORT, new GrpcShortsServerStub());
	}
	
	public static void main(String[] args) {
		try {
			Args.use(args);
			new GrpcShortsServer().start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
}
