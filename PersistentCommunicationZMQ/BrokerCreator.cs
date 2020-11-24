using System;
using System.Text;
using ZeroMQ;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using ZeroMQ.Monitoring;
using System.Threading;
using System.IO;

namespace PersistentCommunicationZMQ
{
    public abstract class BrokerCreator:SendCreator
    {
        static string[] AddressforClientReq = { "tcp://0.0.0.0:5551", "tcp://0.0.0.0:5558", "tcp://0.0.0.0:5565" };
        static string[] AddressforClientPush = { "tcp://0.0.0.0:5553", "tcp://0.0.0.0:5560", "tcp://0.0.0.0:5567" };
        static string[] AddressforClientPub = { "tcp://0.0.0.0:5555", "tcp://0.0.0.0:5562", "tcp://0.0.0.0:5569" };
        public static void SendCopies(List<ArrayList> copies, string brokerIdentity)
        {            
            if (copies.Any())
            {
                string senderAddr = null;
                string receiverAddr = null; 
                
                for (int i = 0; i < copies.Count; i++)
                {
                    string content = copies[i][0].ToString(); 
                    string uid = copies[i][1].ToString();
                    string messageType = copies[i][2].ToString();
                    ZSocket socket = (ZSocket)copies[i][3];

                    if (copies[i].Count>4)
                    {
                        senderAddr = copies[i][4].ToString();
                    }
                    if (copies[i].Count>5)
                    {
                        receiverAddr = copies[i][5].ToString();
                    }

                    Send(socket, Encoding.UTF8.GetBytes(brokerIdentity), content, uid, messageType, senderAddr, receiverAddr);
                }
            }
        }       
        public static void AddToList(List<ArrayList> ReqResCopList, string content, Guid uid, string messageTypeOrIdentity, string senderAddr, string receiverAddr, ZSocket socket, ZSocket socket2 = null)
        {           
            ArrayList list = new ArrayList();
            list.Add(content); 
            list.Add(uid);       
            list.Add(messageTypeOrIdentity);
            list.Add(socket);
            if (senderAddr != null)
            {
                list.Add(senderAddr);
            }
            if (receiverAddr != null)
            {
                list.Add(receiverAddr);
            }
            if(!ReqResCopList.Contains(list)) ReqResCopList.Add(list);

            if (socket2!= null)
            {
                ArrayList list2 = new ArrayList();
                list2.Add(content);
                list2.Add(uid);
                list2.Add(messageTypeOrIdentity);
                list2.Add(socket2);
                if (senderAddr != null)
                {
                    list2.Add(senderAddr);
                }
                if (receiverAddr != null)
                {
                    list2.Add(receiverAddr);
                }
                if (!ReqResCopList.Contains(list2)) ReqResCopList.Add(list2);
            }
        }
        public static void RemoveFromList(List<ArrayList> ReqResList, Guid uid, string messageType = null, ZSocket socket = null)
        {
            if (messageType != null)
            {
                ReqResList.RemoveAll(x => Guid.Parse(x[1].ToString()) == uid & x[2].ToString() == messageType & x[3] == socket);
            }
            else
            {
                ReqResList.RemoveAll(x => Guid.Parse(x[1].ToString()) == uid);
            }
        }
        public static ArrayList GetFromList(List<ArrayList> ReqResList, Guid uid)
        {
            return ReqResList.FirstOrDefault(x => Guid.Parse(x[1].ToString()) == uid);
        }
        public static void Broker(ZContext context, string brokerAddrRequest, string brokerAddrResponse, string brokerAddrRequestPush, string brokerAddrResponsePull, string brokerAddrRequestPub, string brokerAddrResponseSub, string AddrToBroker1, string AddrToBroker2, string AddrFromBrokers, string brokerIdentity)
        {
            List<ArrayList> requestEndpoints = new List<ArrayList>();
            List<ArrayList> responseEndpoints = new List<ArrayList>();
            List<ArrayList> copies = new List<ArrayList>();

            using (var brokerRequest = new ZSocket(context, ZSocketType.ROUTER)) 
            using (var brokerResponse = new ZSocket(context, ZSocketType.DEALER))
            using (var brokerRequestPush = new ZSocket(context, ZSocketType.PULL))
            using (var brokerResponsePull = new ZSocket(context, ZSocketType.PUSH))
            using (var brokerRequestPub = new ZSocket(context, ZSocketType.SUB))
            using (var brokerResponseSub = new ZSocket(context, ZSocketType.PUB))
            using (var brokerToOtherBroker1 = new ZSocket(context, ZSocketType.DEALER))
            using (var brokerToOtherBroker2 = new ZSocket(context, ZSocketType.DEALER))
            using (var brokerFromBrokers = new ZSocket(context, ZSocketType.DEALER))
            {
                brokerRequestPub.SubscribeAll();

                String reqMonitorAddr = "inproc://req."+brokerIdentity;
                brokerRequest.Monitor(reqMonitorAddr);
                ZMonitor reqMonitor = ZMonitor.Create(context, reqMonitorAddr);
                reqMonitor.Start();
                bool reqConnected = false;
                bool reqDisconnected = false;

                reqMonitor.Disconnected +=
                (s, a) =>
                {
                    reqDisconnected = true;
                    reqConnected = false;
                };

                reqMonitor.Accepted +=
                (s, a) =>
                {     
                    reqConnected = true;
                    reqDisconnected = false;
                };

                String subMonitorAddr = "inproc://sub." + brokerIdentity;
                brokerResponseSub.Monitor(subMonitorAddr);
                ZMonitor subMonitor = ZMonitor.Create(context, subMonitorAddr);
                subMonitor.Start();
                bool subConnected = false;
                bool subDisconnected = false;

                subMonitor.Disconnected +=
                (s, a) =>
                {
                    subDisconnected = true;
                    subConnected = false;
                };

                subMonitor.Accepted +=
                (s, a) =>
                {
                    subConnected = true;
                };

                String pullMonitorAddr = "inproc://pull." + brokerIdentity;
                brokerResponsePull.Monitor(pullMonitorAddr);
                ZMonitor pullMonitor = ZMonitor.Create(context, pullMonitorAddr);
                pullMonitor.Start();
                bool pullConnected = false;
                bool pullDisconnected = false;

                pullMonitor.Disconnected +=
                (s, a) =>
                {
                    pullDisconnected = true;
                    pullConnected = false;
                };

                pullMonitor.Accepted +=
                (s, a) =>
                {
                    pullConnected = true;
                };

                brokerRequest.Bind(brokerAddrRequest);
                brokerResponse.Bind(brokerAddrResponse);
                brokerFromBrokers.Bind(AddrFromBrokers);
                brokerToOtherBroker1.Connect(AddrToBroker1);
                brokerToOtherBroker2.Connect(AddrToBroker2);
                brokerRequestPush.Bind(brokerAddrRequestPush);
                brokerResponsePull.Bind(brokerAddrResponsePull);
                brokerRequestPub.Bind(brokerAddrRequestPub);
                brokerResponseSub.Bind(brokerAddrResponseSub);

                ZError error;
                ZMessage request;
                ZPollItem poll = ZPollItem.CreateReceiver();
                
                string fileRequest = "Request.txt";
                string fileResponse = "Response.txt";
                //string pathReq =".\\Request\\" + uid + ".txt";
                //string pathRep = ".\\Request\\" + uid + ".txt";
                Directory.CreateDirectory("Request");
                Directory.CreateDirectory("Response");
                if(!File.Exists(fileRequest)) File.Create(fileRequest).Close();
                if(!File.Exists(fileResponse)) File.Create(fileResponse).Close();

                while (true)
                {
                    SendCopies(copies, brokerIdentity);
                    
                    if (requestEndpoints.Any())
                    {
                        for (int i = 0; i < requestEndpoints.Count; i++)
                        {
                            string content = requestEndpoints[i][0].ToString();
                            string uid = requestEndpoints[i][1].ToString();

                            if (requestEndpoints[i].Count > 5)
                            {
                                string addrOrTopic;
                                if (requestEndpoints[i][5].ToString().Length == 20)
                                {
                                    addrOrTopic = requestEndpoints[i][5].ToString().Substring(16, 4);
                                }
                                else
                                {
                                    addrOrTopic = requestEndpoints[i][5].ToString();
                                }
                                ZSocket socket = (ZSocket)requestEndpoints[i][3];
                                string address = socket.LastEndpoint.Substring(14, 4);
                                bool sended = false;

                                if (addrOrTopic == address)
                                {
                                    if (socket.SocketType == ZSocketType.PUSH & pullDisconnected == false & pullConnected == true)
                                    {
                                        sended = Send(socket, Encoding.UTF8.GetBytes(brokerIdentity), content, uid, "Server");
                                    }
                                    else 
                                    {
                                        sended = Send(socket, Encoding.UTF8.GetBytes(brokerIdentity), content, uid, "Server");
                                    }
                                }
                                else if (socket.SocketType == ZSocketType.PUB & subDisconnected == false & subConnected == true)
                                {
                                    sended = Send(socket, Encoding.UTF8.GetBytes(brokerIdentity), content, uid, "Server", addrOrTopic);
                                }

                                if (socket != brokerResponse && sended == true)
                                {                                   
                                    RemoveFromList(requestEndpoints, Guid.Parse(uid));
                                    string messageType = "REP";

                                    MoveFile(uid, content);
                                    //WriteToFile(fileResponse, Guid.Parse(uid));
                                    DeleteFile(uid);
                                    UpdateFile(fileRequest, uid);

                                    AddToList(copies, content, Guid.Parse(uid), messageType, null, null, brokerToOtherBroker1, brokerToOtherBroker2);
                                }
                            }
                        }
                    }

                    if (responseEndpoints.Any())
                    {
                        for (int i = 0; i < responseEndpoints.Count; i++)
                        {
                            string content = responseEndpoints[i][0].ToString();
                            string uid = responseEndpoints[i][1].ToString();

                            if (responseEndpoints[i].Count > 4)
                            {
                                string identity = responseEndpoints[i][2].ToString();
                                ZSocket socket = (ZSocket)responseEndpoints[i][3];
                                string address = responseEndpoints[i][4].ToString().Substring(14, 4);
                                string address2 = brokerRequest.LastEndpoint.Substring(14, 4);

                                if (address == address2)
                                {
                                    if (reqDisconnected == true & reqConnected == false)
                                    {
                                        Thread.Sleep(5 * 1000);
                                    }

                                    if (reqDisconnected == false & reqConnected == true)
                                    {
                                        Send(socket, Encoding.UTF8.GetBytes(brokerIdentity), content, uid, identity);

                                        DeleteFile(uid);
                                        UpdateFile(fileResponse, uid);
                                        RemoveFromList(responseEndpoints, Guid.Parse(uid));

                                        string messageType = "CON";
                                        AddToList(copies, content, Guid.Parse(uid), messageType, null, null, brokerToOtherBroker1, brokerToOtherBroker2);
                                    }
                                }
                            }
                        }
                    }

                    if (brokerRequest.PollIn(poll, out request, out error, TimeSpan.FromMilliseconds(1)))
                    {
                        string receiverAddress = request[2].ReadString();
                        string identity = request[3].ReadString();
                        string content = request[4].ReadString();
                        string messageType = "REQ";
                        string senderAddress = brokerRequest.LastEndpoint.Substring(0, brokerRequest.LastEndpoint.Length - 1);

                        Guid guid = Guid.NewGuid();
                        Console.WriteLine("{0} <- {1} : [{2} {3}]", brokerIdentity, identity, messageType, guid.ToString());
                        CreateFile(guid, content);
                        WriteToFile(fileRequest, guid);
                        AddToList(requestEndpoints, content, guid, identity, senderAddress, receiverAddress, brokerResponse);
                        AddToList(copies, content, guid, messageType, senderAddress, receiverAddress, brokerToOtherBroker1, brokerToOtherBroker2);
                    }
                    else
                    {
                        ErrorChecker(error);
                    }

                    if (brokerRequestPush.PollIn(poll, out request, out error, TimeSpan.FromMilliseconds(1)))
                    {
                        string receiverAddress = request[0].ReadString(); 
                        string identity = request[1].ReadString();
                        string content = request[2].ReadString();
                        string messageType = "REQ";
                        string senderAddress = brokerRequestPush.LastEndpoint.Substring(0, brokerRequest.LastEndpoint.Length - 1);

                        Guid guid = Guid.NewGuid();
                        Console.WriteLine("{0} <- {1} : [{2}]", brokerIdentity, identity, guid.ToString());
                        CreateFile(guid, content);
                        WriteToFile(fileRequest, guid);
                        AddToList(requestEndpoints, content, guid, identity, senderAddress, receiverAddress, brokerResponsePull);
                        AddToList(copies, content, guid, messageType, senderAddress, receiverAddress, brokerToOtherBroker1, brokerToOtherBroker2);
                    }
                    else
                    {
                        ErrorChecker(error);
                    }

                    if (brokerRequestPub.PollIn(poll, out request, out error, TimeSpan.FromMilliseconds(1)))
                    {
                        string topic = request[0].ReadString();
                        string identity = request[1].ReadString();
                        string content = request[2].ReadString();
                        string messageType = "REQ";
                        string senderAddress = brokerRequestPub.LastEndpoint.Substring(0, brokerRequest.LastEndpoint.Length - 1);

                        Guid guid = Guid.NewGuid();
                        Console.WriteLine("{0} <- {1} : [{2}]", brokerIdentity, identity, guid.ToString());
                        CreateFile(guid, content, topic);
                        WriteToFile(fileRequest, guid);

                        AddToList(requestEndpoints, content, guid, identity, senderAddress, topic, brokerResponseSub);
                        AddToList(copies, content, guid, messageType, senderAddress, topic, brokerToOtherBroker1, brokerToOtherBroker2);
                    }
                    else
                    {                     
                        ErrorChecker(error);
                    }

                    int counter = 1;
                    do
                    {
                        if (brokerFromBrokers.PollIn(poll, out request, out error, TimeSpan.FromMilliseconds(100)))
                        {
                            string identity = request[0].ReadString();
                            string content = request[1].ReadString();
                            string uid = request[2].ReadString();
                            string messageType = request[3].ReadString();
                            string senderAddress = null;
                            string receiverAddress = null;

                            if (request.Count > 5) 
                            {
                                senderAddress = request[4].ReadString();
                                receiverAddress = request[5].ReadString();
                            }
                            else if (request.Count > 4)
                            {
                                senderAddress = request[4].ReadString();                            
                            }

                            ZSocket socket2 = null;
                            ZSocket socket = null;

                            if ((brokerIdentity == "Broker1" & identity == "Broker2") ||
                                (brokerIdentity == "Broker2" & identity == "Broker1") ||
                                (brokerIdentity == "Broker3" & identity == "Broker1"))
                            {
                                socket = brokerToOtherBroker1;
                            }
                            else if ((brokerIdentity == "Broker1" & identity == "Broker3") ||
                                     (brokerIdentity == "Broker2" & identity == "Broker3") ||
                                     (brokerIdentity == "Broker3" & identity == "Broker2"))
                            {
                                socket = brokerToOtherBroker2;
                            }

                            if (messageType == "CONCOP")
                            {
                                RemoveFromList(copies, Guid.Parse(uid), content, socket);
                            }
                            else
                            {
                                if (messageType == "REQ")
                                {
                                    if (AddressforClientReq.Contains(senderAddress))
                                    {
                                        socket2 = brokerResponse;
                                    }
                                    else if (AddressforClientPush.Contains(senderAddress))
                                    {
                                        socket2 = brokerResponsePull;
                                    }
                                    else if (AddressforClientPub.Contains(senderAddress))
                                    {
                                        socket2 = brokerResponseSub;
                                    }

                                    AddToList(requestEndpoints, content, Guid.Parse(uid), identity, senderAddress, receiverAddress, socket2);
                                }
                                else if (messageType == "REP")
                                {
                                    if (AddressforClientReq.Contains(senderAddress))
                                    {
                                        var list = GetFromList(requestEndpoints, Guid.Parse(uid));

                                        if (list!=null)
                                        {
                                            string identity2 = list[2].ToString();
                                            AddToList(responseEndpoints, content, Guid.Parse(uid), identity2, senderAddress, null, brokerRequest);
                                        } 
                                    }

                                    RemoveFromList(requestEndpoints, Guid.Parse(uid));
                                }
                                else if (messageType == "CON")
                                {
                                    RemoveFromList(responseEndpoints, Guid.Parse(uid));
                                }
                                Send(socket, Encoding.UTF8.GetBytes(brokerIdentity), messageType, uid, "CONCOP", null, null);
                            }
                        }
                        else
                        {  
                            ErrorChecker(error);
                        }
                        counter++;
                    }
                    while (copies.Any() & counter < 4);

                    if (brokerResponse.PollIn(poll, out request, out error, TimeSpan.FromMilliseconds(1)))
                    {
                        string uid = request[2].ReadString(); 
                        string identity = request[3].ReadString();
                        string content = request[4].ReadString();
                        string messageType = "REP";
                        ArrayList list = GetFromList(requestEndpoints, Guid.Parse(uid));
                        string address = list[4].ToString();
                        string identity2 = list[2].ToString();

                        Console.WriteLine("{0} <- {1} : [{2} {3}]", brokerIdentity, identity, uid, messageType);
                        MoveFile(uid, content);
                        WriteToFile(fileResponse, Guid.Parse(uid));
                        UpdateFile(fileRequest, uid);
                        RemoveFromList(requestEndpoints, Guid.Parse(uid));
                        AddToList(responseEndpoints, content, Guid.Parse(uid), identity2, address, null, brokerRequest);
                        AddToList(copies, content, Guid.Parse(uid), messageType, address, null, brokerToOtherBroker1, brokerToOtherBroker2);
                    }
                    else
                    {
                        ErrorChecker(error);
                    }
                }
            }
        }
    }
}
