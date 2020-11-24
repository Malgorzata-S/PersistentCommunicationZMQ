using System;
using System.Text;
using ZeroMQ;
using System.Linq;

namespace PersistentCommunicationZMQ
{
    public abstract class SendCreator:FileSystem
    {
        static string[] broker1Address = { "tcp://127.0.0.1:5551", "tcp://127.0.0.1:5553", "tcp://127.0.0.1:5555" };
        static string[] broker2Address = { "tcp://127.0.0.1:5558", "tcp://127.0.0.1:5560", "tcp://127.0.0.1:5562" };
        static string[] broker3Address = { "tcp://127.0.0.1:5565", "tcp://127.0.0.1:5567", "tcp://127.0.0.1:5569" };
        static string[] AddressforServerRep = { "tcp://127.0.0.1:5552", "tcp://127.0.0.1:5559", "tcp://127.0.0.1:5566" };
        public static String GetReceiverIdentity(string brokerAddress)
        {
            string receiverIdentity = null;
            if(broker1Address.Contains(brokerAddress))
            {
                receiverIdentity = "Broker1";
            }
            else if (broker2Address.Contains(brokerAddress))
            {
                receiverIdentity = "Broker2";
            }
            else if (broker3Address.Contains(brokerAddress))
            {
                receiverIdentity = "Broker3";
            }
            
            if (AddressforServerRep[0] == brokerAddress)
            {
                receiverIdentity = "Broker1";
            }
            if (AddressforServerRep[1] == brokerAddress)
            {
                receiverIdentity = "Broker2";
            }
            if (AddressforServerRep[2] == brokerAddress)
            {
                receiverIdentity = "Broker3";
            }

            return receiverIdentity;
        }
        public static void ErrorChecker(ZError error)
        {
            if (error == ZError.ETERM)
                return;    // Interrupted
            if (error != ZError.EAGAIN)
                throw new ZException(error);
        }
        public static bool Send(ZSocket socket, byte[] identity, string message, string addrOrTopicOrID = null, string receiverID = null, string topic = null)
        {
            ZError error;

            using (var outgoing = new ZMessage())
            {
                if (receiverID != null & topic == null)
                    outgoing.Add(new ZFrame(receiverID)); 
                if (topic != null)
                    outgoing.Add(new ZFrame(topic)); 
                if (socket.SocketType == ZSocketType.ROUTER || socket.SocketType == ZSocketType.DEALER)
                    outgoing.Add(new ZFrame());
                if(addrOrTopicOrID!= null)
                    outgoing.Add(new ZFrame(addrOrTopicOrID));
                outgoing.Add(new ZFrame(identity));
                outgoing.Add(new ZFrame(message));

                if (!socket.Send(outgoing, ZSocketFlags.DontWait, out error))
                {
                    ErrorChecker(error);
                    return false;
                }

                string brokerAddress = socket.LastEndpoint.Substring(0, socket.LastEndpoint.Length - 1);
                string receiverIdentity2 = GetReceiverIdentity(brokerAddress);
                string messageType = null;

                if (socket.SocketType == ZSocketType.REP)
                { 
                    Console.WriteLine("{0} -> {1} : [{2} {3}]", Encoding.UTF8.GetString(identity), receiverIdentity2, addrOrTopicOrID, "REP");
                }
                else
                {
                    if (socket.SocketType == ZSocketType.ROUTER)
                    {
                        messageType = "REP";
                    }
                    else
                    {
                        messageType = "REQ";
                    }

                    if (topic != null)
                    {
                        Console.WriteLine("{0} -> : [{1}]", Encoding.UTF8.GetString(identity), topic);
                    }
                    else if (socket.SocketType == ZSocketType.PUSH && receiverID != null)
                    {
                        Console.WriteLine("{0} -> {1} : [{2}]", Encoding.UTF8.GetString(identity), receiverID, addrOrTopicOrID);
                    }
                    else if (receiverID != null)
                    {
                        Console.WriteLine("{0} -> {1} : [{2} {3}]", Encoding.UTF8.GetString(identity), receiverID, addrOrTopicOrID, messageType);
                    }
                    else if(socket.SocketType == ZSocketType.PUSH)
                    {
                        Console.WriteLine("{0} -> {1} : []", Encoding.UTF8.GetString(identity), receiverIdentity2);
                    }
                    else if (socket.SocketType == ZSocketType.PUB)
                    {
                        Console.WriteLine("{0} -> {1} : [{2}]", Encoding.UTF8.GetString(identity), receiverIdentity2, addrOrTopicOrID);
                    }
                    else 
                    {
                        Console.WriteLine("{0} -> {1} : [{2}]", Encoding.UTF8.GetString(identity), receiverIdentity2, messageType);
                    }
                }
            }
            return true;
        }
        public static bool Send(ZSocket socket, byte[] identity, string message, string uid, string messageType, string senderAddr, string receiverAddr)
        {                 
            ZError error;
            using (var outgoing = new ZMessage())
            {
                outgoing.Add(new ZFrame(identity));
                outgoing.Add(new ZFrame(message));
                outgoing.Add(new ZFrame(uid));
                outgoing.Add(new ZFrame(messageType));
                if (senderAddr != null)
                {
                    outgoing.Add(new ZFrame(senderAddr));
                }
                if (receiverAddr != null)
                {
                    outgoing.Add(new ZFrame(receiverAddr));
                }

                if (!socket.Send(outgoing, ZSocketFlags.DontWait, out error))
                {
                    ErrorChecker(error);
                    return false;
                }
                return true;
            }
        }
    }
}
