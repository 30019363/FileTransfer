
/**
 * GoBackFtp Class
 * 
 * GoBackFtp implements a basic FTP application based on UDP data transmission.
 * It implements a Go-Back-N protocol. The window size is an input parameter.
 * 
 * @author 	Joshua Kim
 * @version	April, 2021
 *
 */

import java.io.DataInputStream; 
import java.io.DataOutputStream; 
import java.io.FileInputStream;
import java.io.File; 
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.*;


public class GoBackFtp {
	// global logger	
	private static final Logger logger = Logger.getLogger("GoBackFtp");
	// class variables 
	private String fileName;
	private int rtoTimer, localPort, udpPort, seqNum, ackNum, windowSize;
	private Timer timer; 
	private Timeout retx; // object storing the timer task
	private FtpSegment segment;
	private File file; // file to be transmitted
	private DatagramSocket udpSocket;
	private static DatagramPacket pkt;
	private ConcurrentLinkedQueue<DatagramPacket> transitQueue = new ConcurrentLinkedQueue<DatagramPacket>();
	private DatagramPacket received;
	private FtpSegment ack;
	private Socket tcpSocket;
	private static InetAddress ip;
	private long fileSize;
	private Thread receiveACK, sendData;

	/**
	 * Constructor to initialize the program 
	 * 
	 * @param windowSize	Size of the window for Go-Back_N in units of segments
	 * @param rtoTimer		The time-out interval for the retransmission timer
	 */
	public GoBackFtp(int windowSize, int rtoTimer) {
		this.windowSize = windowSize;
		this.rtoTimer = rtoTimer;
		retx = new Timeout();
	}


	/**
	 * Send the specified file to the specified remote server
	 * 
	 * @param serverName	Name of the remote server
	 * @param serverPort	Port number of the remote server
	 * @param fileName		Name of the file to be trasferred to the rmeote server
	 * @throws FtpException If unrecoverable errors happen during file transfer
	 */
	public void send(String serverName, int serverPort, String fileName) throws FtpException {
		this.fileName = fileName;
		try {
			file = new File(fileName); // finds the file to be sent
			udpSocket = new DatagramSocket(); // connect to a random available port 
			tcpSocket = new Socket(serverName, serverPort); // connects to server 

			// gets servers up address
			ip = InetAddress.getByName(serverName);
			// conducts handshake over tcp 
			handshake();

			// creates the timer 
			timer = new Timer();
			udpSocket.connect(ip, udpPort); // opens udp connection to server

			createThreads(); // creates the send data and receive ack threads

			// starts threads
			receiveACK.start();
			sendData.start();

			// gives the threads time to finish currently executing tasks
			sendData.join();
			receiveACK.join();

			timer.cancel(); // allow timer task to terminate gracefully
			timer.purge(); // remove canceled tasks from timer

			// close connection
			udpSocket.close();

		} catch (FileNotFoundException e) {
			throw new FtpException();
		} catch (SocketException e) {
			throw new FtpException();
		} catch (SecurityException e) {
			throw new FtpException();
		} catch (UnknownHostException e) {
			throw new FtpException();
		} catch (IOException e) {
			throw new FtpException();
		} catch (FtpException e) {
			throw new FtpException();
		} catch (InterruptedException e) {
			throw new FtpException();
		}
	}

	/**
	 * Creates new timer task and starts a new timer for the new datagram packet
	 */
	private synchronized void startTimer() {
		retx = new Timeout();
		timer.scheduleAtFixedRate(retx, rtoTimer, rtoTimer);
	}

	/**
	 * Cancels the timer task and purges it out of the timer
	 */
	private synchronized void stopTimer() {
		retx.cancel();
		timer.purge();
	}

	
	/**
	 * Helper method to execute the handshake over TCP 
	 * 
	 * @param fileName name of file/filepath to be sent to server
	 */
	private void handshake() throws FtpException {
		try {
			fileSize = file.length();
			localPort = udpSocket.getLocalPort();
			DataInputStream input = new DataInputStream(tcpSocket.getInputStream());
			DataOutputStream output = new DataOutputStream(tcpSocket.getOutputStream());

			// sends file name, file size, and local UDP port number to server
			output.writeUTF(fileName);
			output.writeLong(fileSize);
			output.writeInt(localPort);
			output.flush();
			// receives UDP port number and initial sequence number from server
			udpPort = input.readInt();
			seqNum = input.readInt(); 
			// end of handshake
			tcpSocket.close(); // closes TCP connection 
		} catch (IOException e) {
			throw new FtpException();
		} catch (SecurityException e) {
			throw new FtpException();
		}
	}

	/**
	 * Helper method used to create the two threads used to send file data to the server and 
	 * receive ack's back from the server 
	 * 
	 */
	private void createThreads() {

		// create ack receiving thread
		receiveACK = new Thread() {
			@Override
			public void run() {
				try {
					FtpSegment seg = new FtpSegment(0, new byte[1400]);
					received = FtpSegment.makePacket(seg, ip, udpPort); // packet for received ack to be contained
					udpSocket.setSoTimeout(500); // 0.5 seconds, just in case multiple packets are lost
					while (sendData.isAlive() || !(transitQueue.isEmpty())) {
						udpSocket.receive(received); // listens for ack
						stopTimer(); // stops timer once ack is received
						ack = new FtpSegment(received);
						ackNum = ack.getSeqNum();
						System.out.println("ack " + ackNum); // prints ack number
						for (DatagramPacket p : transitQueue) { // cycles through each packet in the queue
							seg = new FtpSegment(p);
							if (ackNum > seg.getSeqNum()) // checks if packet has been ack'ed
								transitQueue.remove(p); // removes from window
						}
						if (!transitQueue.isEmpty()) 
							startTimer(); // restarts the timer
					}
				} catch (SocketTimeoutException e) {
					// if method is stuck in receive method
				} catch (IOException e) {
					e.printStackTrace();
				} 
				
			}
		};

		// create data sending thread
		sendData = new Thread() {
			@Override
			public void run() {
				try {
					FileInputStream in = new FileInputStream(file); // opens input file
					int size = 0;
					byte[] buff = new byte[1400];
					while ((size = in.read(buff)) != -1) { // reads from file
						segment = new FtpSegment(seqNum, buff, size); // creates data segment to be sent
						pkt = FtpSegment.makePacket(segment, ip, udpPort); // converts segment to packet
						while (transitQueue.size() == windowSize) {
							// wait 
							yield();
						}
						transitQueue.add(pkt); // add packet to queue/window
						udpSocket.send(pkt); // send packet to server
						System.out.println("send " + seqNum);

						if (transitQueue.peek().equals(pkt)) { // head packet = most recent packet 
							startTimer();
						}
						seqNum++;
					}
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		};
	}

	/**
	 * Simple timertask used to retransmit all packets in the window in case of a timeout 
	 */
	private class Timeout extends TimerTask {

		@Override
		public void run() {
			if (transitQueue.isEmpty()) return; // do nothing if there is nothing to retransmit
			try {
				System.out.println("timeout");
				for (DatagramPacket p : transitQueue) { // cycles through all packets in window
					System.out.println("retx " + new FtpSegment(p).getSeqNum());
					udpSocket.send(p);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}
} // end of class