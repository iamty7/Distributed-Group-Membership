import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

/*
 * MP2 - Distributed Group Membership
 */

public class DGM {

	/*
	 * Node in the membership list and messages
	 */
	private class Node {
		private String hostName;
		private String timestamp;
		private int hashID;

		/*
		 * Constructor of Class Node
		 */
		public Node() {
			this.timestamp = getTimestamp();
			this.hostName = getHostName();

			this.hashID = getHashID(new String(this.hostName + this.timestamp));
			while (membershipList[hashID] != null) {
				hashID = (hashID + 1) % 128;
			}
		}
	}

	/*
	 * Thread for heartbeating
	 */
	private class HeartBeatThread implements Runnable {

		@Override
		public void run() {
			try {
				while (!goingToLeave) {
					synchronized (successors) {
						for (Node node : successors) {
							sendMessage(node.hostName, HEARTBEAT, thisNode);
						}
					}

					Thread.currentThread().sleep(200);
				}
			} catch (InterruptedException e) {

			} catch (UnknownHostException e) {
				// e.printStackTrace();
				// System.out.println("HEARTBEAT target unknown: " +
				// node.hostName);
			}
		}

	}

	/*
	 * Thread for handling the received messages
	 */
	private class MessageHandlerThread implements Runnable {

		@Override
		public void run() {
			byte[] buf = new byte[1024];

			try {
				DatagramSocket ds = new DatagramSocket(SERVER_PORT);
				DatagramPacket dp_receive = new DatagramPacket(buf, 1024);
				while (!goingToLeave) {
					ds.receive(dp_receive);
					String[] message = new String(dp_receive.getData()).split("\n");
					int messageType = Integer.parseInt(message[0]);
					Node node = new Node();
					node.hostName = message[1];
					node.timestamp = message[2];
					node.hashID = Integer.parseInt(message[3]);
					// System.out.println("New Message Received! " +
					// messageType);
					switch (messageType) {
					case HEARTBEAT:
						updateHBRecord(node);
						break;
					case JOIN:
						newGroupMember(node);
						break;
					case MEMBERSHIPLIST:
						initMembershipList(node);
						break;
					case NEWMEMBER:
						addMember(node);
						break;
					case LEAVE:
						memberLeave(node);
						break;
					case FAILURE:
						markAsFailure(node.hashID);
						break;
					default:
						break;
					}
				}
				ds.close();
			} catch (SocketException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/*
	 * Thread for failure detection
	 */
	private class FailureDetectorThread implements Runnable {

		@Override
		public void run() {

			try {
				while (!goingToLeave) {

					for (Integer key : heartbeatCnt.keySet()) {
						long curTime = System.currentTimeMillis();
						if (curTime - heartbeatCnt.get(key) > 1000) {
							markAsFailure(key);
						}
					}

					Thread.currentThread().sleep(1000);
				}
			} catch (InterruptedException e) {

			}
		}

	}

	/*
	 * Constructor of Class DGM
	 */
	public DGM() {
		// implemented in the initializeDGM method
	}

	// message type
	private final int HEARTBEAT = 0;
	private final int JOIN = 1;
	private final int MEMBERSHIPLIST = 2;
	private final int NEWMEMBER = 3;
	private final int LEAVE = 4;
	private final int FAILURE = 5;


	private final int SERVER_PORT = 8399;

	// member-variables
	private boolean is_introducer;
	private Node[] membershipList;
	private Node thisNode;
	private List<Node> successors;
	private List<Node> predecessors;
	private boolean in_group;
	private boolean goingToLeave;
	private Thread heartBeatThread;
	private Thread messageHandlerThread;
	private Thread failureDetectorThread;
	private ConcurrentHashMap<Integer, Long> heartbeatCnt;

	public static void main(String[] args) {

		DGM dgm = new DGM();
		dgm.start();
	}

	/*
	 * Starts to take commands from users
	 */
	private void start() {
		Scanner keyboard = new Scanner(System.in);
		String command;
		while (true) {
			System.out.print("Your command(join, leave, showlist, showid):");
			command = keyboard.nextLine();
			switch (command) {
			case "join":
				joinGroup();
				break;
			case "leave":
				leaveGroup();
				break;
			case "showlist":
				showList();
				break;
			case "showid":
				showID();
				break;
			default:
				break;
			}
		}

	}

	/*
	 * Show the ID of the node(HostName and its time stamp)
	 */
	private void showID() {
		if (in_group)
			System.out.println("ID: " + thisNode.hostName + " " + thisNode.timestamp);
		else
			System.out.println("Please join the group first!");
	}

	/*
	 * Show the membership list of the node
	 */
	private void showList() {
		if (in_group) {
			System.out.println("Membership list of " + thisNode.hostName + "=======");
			synchronized (failureDetectorThread) {
				for (Node node : membershipList) {
					if (node != null)
						System.out.println(node.hostName + " " + node.timestamp);
				}
			}

		} else
			System.out.println("Please join the group first!");
	}

	/*
	 * Leave the group and send the leave message to its successors
	 */
	private void leaveGroup() {
		if (!in_group) {
			System.out.println("You haven't joined a group!");
			return;
		}

		// DatagramSocket ds = new DatagramSocket(SERVER_PORT);
		// Send the leave message to the successors
		synchronized (successors) {
			for (Node successor : successors) {
				try {
					sendMessage(successor.hostName, LEAVE, thisNode);
				} catch (UnknownHostException e) {
					e.printStackTrace();
					System.out.println("Can't find the destination host: " + successor.hostName);
				}

			}
		}
		nodeReset();
	}

	/*
	 * Send a request to the introducer
	 */
	private void joinGroup() {
		if (in_group) {
			System.out.println("This node is already in the group!");
			return;
		}

		if (heartBeatThread == null) {
			initializeDGM();
			heartBeatThread.start();
			messageHandlerThread.start();
			failureDetectorThread.start();
		}

		if (!is_introducer) {
			try {
				sendMessage("fa17-cs425-g15-01.cs.illinois.edu", JOIN, thisNode);
			} catch (UnknownHostException e) {
				e.printStackTrace();
				// System.out.println("The introducer is down! Can't join the
				// group!");
			}
		} else {
			if (!searchGroup()) {
				System.out.println("I'm the introducer. Group created!");
				in_group = true;
			}
			else
				System.out.println("Rejoin the group");
		}
	}

	/*
	 * Search the existing group in the network
	 */
	private boolean searchGroup() {
		String s1 = "fa17-cs425-g15-";
		String s2 = ".cs.illinois.edu";
		for (int i = 2; i <= 10; i++) {
			try {
				if (i < 10)
					sendMessage(s1 + "0" + i + s2, JOIN, thisNode);
				else
					sendMessage(s1 + i + s2, JOIN, thisNode);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			try {
				Thread.currentThread().sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (in_group) {
				return true;
			}
		}
		return false;
	}

	/*
	 * Initialize member variables of this node
	 * 
	 * @param string: host name
	 */
	private void initializeDGM() {
		// Judge if this vm is the introducer
		if (getHostName().equals("fa17-cs425-g15-01.cs.illinois.edu"))
			is_introducer = true;
		else
			is_introducer = false;

		membershipList = new Node[128];
		thisNode = new Node();
		successors = new ArrayList<>();
		predecessors = new ArrayList<>();
		heartbeatCnt = new ConcurrentHashMap<>();
		// numOfMembers = 1;
		// in_group = true;
		goingToLeave = false;
		heartBeatThread = new Thread(new HeartBeatThread());
		messageHandlerThread = new Thread(new MessageHandlerThread());
		failureDetectorThread = new Thread(new FailureDetectorThread());

		membershipList[thisNode.hashID] = thisNode;
	}

	/*
	 * Send a message to another node
	 * 
	 * @param destination: the host name of the target node
	 * 
	 * @param messageType: heartbeat, join, membershiplist, newmember, leave or
	 * failure
	 * 
	 * @param node: the node of the event
	 */
	private void sendMessage(String destination, int messageType, Node node) throws UnknownHostException {
		DatagramSocket ds;
		try {
			ds = new DatagramSocket();
			InetAddress serverAddress = InetAddress.getByName(destination);
			String message = nodeToMessage(messageType, node);
			DatagramPacket dp_send = new DatagramPacket(message.getBytes(), message.length(), serverAddress,
					SERVER_PORT);
			ds.send(dp_send);
			ds.close();
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/*
	 * 
	 */
	private void nodeReset() {
		// heartBeatThread.interrupt();
		// messageHandlerThread.interrupt();
		// failureDetectorThread.interrupt();
		goingToLeave = true;
		in_group = false;
		try {
			Thread.currentThread().sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		heartBeatThread = null;
	}

	/*
	 * Convert the message and the node information to a string of message
	 * 
	 * @param messageType: type of the message
	 * 
	 * @param node: the node of the event
	 */
	private String nodeToMessage(int messageType, Node node) {
		String message = messageType + "\n" + node.hostName + "\n" + node.timestamp + "\n" + node.hashID + "\n";
		return message;
	}

	/*
	 * Delete the node from local membership list and send the leave message to
	 * its successors
	 */
	public void memberLeave(Node node) {
		if (membershipList[node.hashID] != null) {
			synchronized (successors) {
				for (Node successor : successors) {
					try {
						sendMessage(successor.hostName, LEAVE, node);
					} catch (UnknownHostException e) {
						e.printStackTrace();
					}
				}
			}

			membershipList[node.hashID] = null;
			selectNeighbors();
			writeLogs("Member Leave:", node);
		}
	}

	/*
	 * Add the node to the local membership list and send the new group member
	 * message to its successors
	 */
	public void addMember(Node node) {
		if (membershipList[node.hashID] == null) {
			membershipList[node.hashID] = node;
			synchronized (successors) {
				for (Node successor : successors) {
					try {
						sendMessage(successor.hostName, NEWMEMBER, node);
					} catch (UnknownHostException e) {
						e.printStackTrace();
					}
				}
			}

			writeLogs("New Member:", node);
			selectNeighbors();
		}
	}

	public void initMembershipList(Node node) {
		in_group = true;
		synchronized (membershipList) {
			membershipList[node.hashID] = node;
		}
		selectNeighbors();
	}

	public void newGroupMember(Node node) {
		synchronized (membershipList) {
			for (Node member : membershipList) {
				try {
					if (member != null) {
						sendMessage(node.hostName, MEMBERSHIPLIST, member);
					}
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
			membershipList[node.hashID] = node;
		}

		synchronized (successors) {
			for (Node successor : successors) {
				try {
					sendMessage(successor.hostName, NEWMEMBER, node);
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
		}

		selectNeighbors();

		writeLogs("New Member:", node);
	}

	public void updateHBRecord(Node node) {
		synchronized (heartbeatCnt) {
			if (heartbeatCnt.containsKey(node.hashID)) {
				heartbeatCnt.put(node.hashID, System.currentTimeMillis());
			}
		}

	}

	/*
	 * Mark a node as failure and the send the failure message to its successors
	 */
	public void markAsFailure(Integer hashID) {
		if (membershipList[hashID] != null) {
			Node node = membershipList[hashID];
			synchronized (heartbeatCnt) {
				heartbeatCnt.remove(hashID);
			}

			synchronized (membershipList) {
				membershipList[hashID] = null;

			}

			selectNeighbors();

			synchronized (successors) {
				for (Node successor : successors) {
					try {
						sendMessage(successor.hostName, FAILURE, node);
					} catch (UnknownHostException e) {
						e.printStackTrace();
					}
				}
			}

			writeLogs("Failure:", node);
		}

	}

	/*
	 * Select successors and predecessors according to the new membership list
	 * and change heart beat target
	 */
	private void selectNeighbors() {
		synchronized (membershipList) {
			successors.clear();
			int index = (thisNode.hashID + 1) % 128;
			while (successors.size() < 5 && index != thisNode.hashID) {
				if (membershipList[index] != null)
					successors.add(membershipList[index]);
				index = (index + 1) % 128;
			}

			predecessors.clear();
			index = thisNode.hashID == 0 ? 127 : thisNode.hashID - 1;
			while (predecessors.size() < 5 && index != thisNode.hashID) {
				if (membershipList[index] != null)
					predecessors.add(membershipList[index]);
				index = index == 0 ? 127 : index - 1;
			}
		}
		synchronized (heartbeatCnt) {
			ConcurrentHashMap<Integer, Long> temp = heartbeatCnt;
			synchronized (predecessors) {
				heartbeatCnt = new ConcurrentHashMap<>();
				for (Node predecessor : predecessors) {
					if (temp.containsKey(predecessor.hashID))
						heartbeatCnt.put(predecessor.hashID, temp.get(predecessor.hashID));
					else {
						heartbeatCnt.put(predecessor.hashID, System.currentTimeMillis());
					}

				}
			}

		}

	}

	private void writeLogs(String event, Node node) {
		try {
			FileWriter writer = new FileWriter(getHostName() + ".log", true);
			writer.write(
					event + "\t" + node.hostName + "\t" + new Timestamp(System.currentTimeMillis()).toString() + "\n");
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Get the host name of the node
	 * 
	 * @return string of host name
	 */
	private String getHostName() {
		return System.getenv("HOSTNAME");
	}

	/*
	 * Hash the input String and module by 128
	 * 
	 * @param string: combination of the timestamp and hostName of a node
	 * 
	 * @return hash results of the string module by 128 as hashID of the node
	 */
	public int getHashID(String ID) {
		int hashID = 7;
		for (int i = 0; i < ID.length(); i++) {
			hashID = hashID * 31 + ID.charAt(i);
		}
		return Math.abs(hashID % 128);

	}

	/*
	 * Get local time as timestamp
	 * 
	 * @return timestamp as string
	 */
	public String getTimestamp() {
		// Timestamp has millis while SimpleDateFormat not
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		return timestamp.toString();
	}

}
