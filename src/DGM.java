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
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

/*
 * MP2 - Distributed Group Membership
 */

public class DGM {

	/*
	 * Node in the membership list
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

	private class HeartBeatThread implements Runnable {

		@Override
		public void run() {
			try {
				while (in_group) {
					for (Node node : successors) {
						sendMessage(node.hostName, HEARTBEAT, thisNode);
					}
					Thread.currentThread().sleep(200);
				}
			} catch (InterruptedException e) {
				// interrupted by the main thread
			} catch (UnknownHostException e) {
				// e.printStackTrace();
				// System.out.println("HEARTBEAT target unknown: " +
				// node.hostName);
			}
		}

	}

	private class MessageHandlerThread implements Runnable {

		@Override
		public void run() {
			byte[] buf = new byte[1024];

			try {
				DatagramSocket ds = new DatagramSocket(SERVER_PORT);
				DatagramPacket dp_receive = new DatagramPacket(buf, 1024);
				while (in_group) {
					ds.receive(dp_receive);
					String[] message = new String(dp_receive.getData()).split("\n");
					int messageType = Integer.parseInt(message[0]);
					Node node = new Node();
					node.hostName = message[1];
					node.timestamp = message[2];
					node.hashID = Integer.parseInt(message[3]);
					//System.out.println("New Message Received! " + messageType);
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
					// int cnt = 0;
					// for (Node member : membershipList) {
					// if (member != null)
					// cnt++;
					// }
					// System.out.println(cnt + "members!!!!!!!");
				}
				ds.close();
			} catch (SocketException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private class FailureDetectorThread implements Runnable {

		@Override
		public void run() {

			try {
				while (in_group) {
					for (Integer key : heartbeatCnt.keySet()) {
						long curTime = System.currentTimeMillis();
						if (curTime - heartbeatCnt.get(key) > 1000) {
							System.out.println("Failure Detected! " + membershipList[key].hostName);
							markAsFailure(key);
						}
					}
					Thread.currentThread().sleep(1000);
				}
			} catch (InterruptedException e) {
				// interrupted by the main thread
			}
		}

	}

	/*
	 * Constructor of Class DGM
	 */
	public DGM() {

	}

	public void memberLeave(Node node) {
		if (membershipList[node.hashID] != null) {
			System.out.println("Node Leaving!" + membershipList[node.hashID].hostName);
			for (Node successor : successors) {
				try {
					sendMessage(successor.hostName, LEAVE, node);
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
			membershipList[node.hashID] = null;
			selectNeighbors();
			writeLogs("Member Leave:", node);
		}
	}

	public void addMember(Node node) {
		if (membershipList[node.hashID] == null) {
			membershipList[node.hashID] = node;
			System.out.println("New Member! " + membershipList[node.hashID].hostName);
			for (Node successor : successors) {
				try {
					sendMessage(successor.hostName, NEWMEMBER, node);
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
			writeLogs("New Member:", node);
			selectNeighbors();
		}
	}

	public void initMembershipList(Node node) {
		membershipList[node.hashID] = node;
		selectNeighbors();
	}

	public void newGroupMember(Node node) {
		for (Node member : membershipList) {
			try {
				if (member != null) {
					System.out.println("sending membership list: " + member.hostName);
					sendMessage(node.hostName, MEMBERSHIPLIST, member);
				}
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}

		membershipList[node.hashID] = node;

		for (Node successor : successors) {
			try {
				System.out.println("Sending new group to " + successor.hostName);
				sendMessage(successor.hostName, NEWMEMBER, node);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		selectNeighbors();

		writeLogs("New Member:", node);
	}

	public void updateHBRecord(Node node) {
		if (heartbeatCnt.containsKey(node.hashID)) {
			heartbeatCnt.put(node.hashID, System.currentTimeMillis());
		}
	}

	public void markAsFailure(Integer hashID) {
		if (membershipList[hashID] != null) {
			Node node = membershipList[hashID];
			heartbeatCnt.remove(hashID);
			membershipList[hashID] = null;

			selectNeighbors();

			for (Node successor : successors) {
				try {
					sendMessage(successor.hostName, FAILURE, node);
				} catch (UnknownHostException e) {
					e.printStackTrace();
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

		HashMap<Integer, Long> temp = heartbeatCnt;
		heartbeatCnt = new HashMap<>();
		for (Node predecessor : predecessors) {
			if (temp.containsKey(predecessor.hashID))
				heartbeatCnt.put(predecessor.hashID, temp.get(predecessor.hashID));
			else {
				heartbeatCnt.put(predecessor.hashID, System.currentTimeMillis());
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
	// private int numOfMembers;
	private Node thisNode;
	private List<Node> successors;
	private List<Node> predecessors;
	private boolean in_group;
	private Thread heartBeatThread;
	private Thread messageHandlerThread;
	private Thread failureDetectorThread;
	private HashMap<Integer, Long> heartbeatCnt = new HashMap<>();

	public static void main(String[] args) {

		// !!!! if the hostname is the domain name, we do not need to pass
		// args
		// to the initialization to decide weather it's the introducer
		DGM dgm = new DGM();
		dgm.start();
	}

	/*
	 *  
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

	private void showID() {
		if (in_group)
			System.out.println("ID: " + thisNode.hostName + " " + thisNode.timestamp);
		else
			System.out.println("Please join the group first!");
	}

	private void showList() {
		if (in_group) {
			System.out.println("Membership list of " + thisNode.hostName + "=======");
			for (Node node : membershipList) {
				if (node != null)
					System.out.println(node.hostName + " " + node.timestamp);
			}
		} else
			System.out.println("Please join the group first!");
	}

	private void leaveGroup() {
		if (!in_group) {
			System.out.println("You haven't joined a group!");
			return;
		}

		// DatagramSocket ds = new DatagramSocket(SERVER_PORT);
		// Send the leave message to the successors
		for (Node successor : successors) {
			try {
				sendMessage(successor.hostName, LEAVE, thisNode);
			} catch (UnknownHostException e) {
				e.printStackTrace();
				System.out.println("Can't find the destination host: " + successor.hostName);
			}

		}
		nodeReset();

	}

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

	private void nodeReset() {
		// heartBeatThread.interrupt();
		// messageHandlerThread.interrupt();
		// failureDetectorThread.interrupt();
		in_group = false;
	}

	private String nodeToMessage(int messageType, Node node) {
		String message = messageType + "\n" + node.hostName + "\n" + node.timestamp + "\n" + node.hashID + "\n";
		return message;
	}

	private void joinGroup() {
		if (in_group) {
			System.out.println("This node is already in the group!");
			return;
		}
		initializeDGM();
		heartBeatThread.start();
		messageHandlerThread.start();
		failureDetectorThread.start();
		if (!is_introducer) {
			try {
				sendMessage("fa17-cs425-g15-01.cs.illinois.edu", JOIN, thisNode);
			} catch (UnknownHostException e) {
				e.printStackTrace();
				System.out.println("The introducer is down! Can't join the group!");
			}
		} else
			System.out.println("I'm the introducer. Group created!");
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
		// numOfMembers = 1;
		in_group = true;
		heartBeatThread = new Thread(new HeartBeatThread());
		messageHandlerThread = new Thread(new MessageHandlerThread());
		failureDetectorThread = new Thread(new FailureDetectorThread());

		membershipList[thisNode.hashID] = thisNode;

		// if not introducer, send a request to the introducer for joining
		// the
		// group

		// if (!is_introducer) {
		// List<Node> memberList = sendJoinReq(thisNode);
		// for (Node node : memberList) {
		// membershipList.add(node.hashID, node);
		// numOfMembers++;
		// }
		//
		// }
	}

	/*
	 * Send joining request to the introducer
	 * 
	 * @param node: node information about itself
	 * 
	 * @return the membership list of the introducer
	 */

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
		// Timestamp has millis while SimpleDateFormat don't
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		return timestamp.toString();
	}

}
