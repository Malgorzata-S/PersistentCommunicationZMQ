using System;
using ZeroMQ;
using System.Threading;

namespace PersistentCommunicationZMQ
{
    class Program
	{
        static string [] AddressforClientReq = {"tcp://127.0.0.1:5551", "tcp://127.0.0.1:5558", "tcp://127.0.0.1:5565"};
        static string [] AddressforServerRep = {"tcp://127.0.0.1:5552", "tcp://127.0.0.1:5559", "tcp://127.0.0.1:5566"};
        static string [] AddressforClientPush = {"tcp://127.0.0.1:5553", "tcp://127.0.0.1:5560", "tcp://127.0.0.1:5567"};
        static string [] AddressforServerPull = {"tcp://127.0.0.1:5554", "tcp://127.0.0.1:5561", "tcp://127.0.0.1:5568"};
        static string [] AddressforClientPub = {"tcp://127.0.0.1:5555", "tcp://127.0.0.1:5562", "tcp://127.0.0.1:5569"};
        static string [] AddressforServerSub = {"tcp://127.0.0.1:5556", "tcp://127.0.0.1:5563", "tcp://127.0.0.1:5570"};
        static string brokerIdentity1 = "Broker1";
        static string brokerIdentity2 = "Broker2";
        static string brokerIdentity3 = "Broker3";
        static string broker1AddrRequest = "tcp://*:5551";
        static string broker1AddrResponse = "tcp://*:5552";
        static string broker1AddrRequestPush = "tcp://*:5553";
        static string broker1AddrResponsePull = "tcp://*:5554";
        static string broker1AddrRequestPub = "tcp://*:5555";
        static string broker1AddrResponseSub = "tcp://*:5556";
        static string broker1AddrFromBrokers = "tcp://*:5557";
        static string broker2AddrRequest = "tcp://*:5558";
        static string broker2AddrResponse = "tcp://*:5559";
        static string broker2AddrRequestPush = "tcp://*:5560";
        static string broker2AddrResponsePull = "tcp://*:5561";
        static string broker2AddrRequestPub = "tcp://*:5562";
        static string broker2AddrResponseSub = "tcp://*:5563";
        static string broker2AddrFromBrokers = "tcp://*:5564";
        static string broker3AddrRequest = "tcp://*:5565";
        static string broker3AddrResponse = "tcp://*:5566";
        static string broker3AddrRequestPush = "tcp://*:5567";
        static string broker3AddrResponsePull = "tcp://*:5568";
        static string broker3AddrRequestPub = "tcp://*:5569";
        static string broker3AddrResponseSub = "tcp://*:5570";
        static string broker3AddrFromBrokers = "tcp://*:5571";
        static string AddrToBroker1 = "tcp://127.0.0.1:5557";
        static string AddrToBroker2 = "tcp://127.0.0.1:5564";
        static string AddrToBroker3 = "tcp://127.0.0.1:5571";
        static void Main(string[] args)
		{
            while (true)
            {
                var context = new ZContext();
                Thread broker1 = new Thread(() => BrokerCreator.Broker(context, broker1AddrRequest, broker1AddrResponse, broker1AddrRequestPush, broker1AddrResponsePull, broker1AddrRequestPub, broker1AddrResponseSub, AddrToBroker2, AddrToBroker3, broker1AddrFromBrokers, brokerIdentity1));
                Thread broker2 = new Thread(() => BrokerCreator.Broker(context, broker2AddrRequest, broker2AddrResponse, broker2AddrRequestPush, broker2AddrResponsePull, broker2AddrRequestPub, broker2AddrResponseSub, AddrToBroker1, AddrToBroker3, broker2AddrFromBrokers, brokerIdentity2));
                Thread broker3 = new Thread(() => BrokerCreator.Broker(context, broker3AddrRequest, broker3AddrResponse, broker3AddrRequestPush, broker3AddrResponsePull, broker3AddrRequestPub, broker3AddrResponseSub, AddrToBroker1, AddrToBroker2, broker3AddrFromBrokers, brokerIdentity3));
                ZSocketType socketTypeClient = ZSocketType.None;
                ZSocketType socketTypeServer = ZSocketType.None;
                string AddressforClient = "";
                string AddressforServer = "";
                ZSocketType socketTypeClient2 = ZSocketType.None;
                ZSocketType socketTypeServer2 = ZSocketType.None;
                string AddressforClient2 = "";
                string AddressforServer2 = "";
                ZSocketType socketTypeClient3 = ZSocketType.None;
                ZSocketType socketTypeServer3 = ZSocketType.None;
                string AddressforClient3 = "";
                string AddressforServer3 = "";
                Thread Client2 = null;
                Thread Server2 = null;
                Thread Client3 = null;
                Thread Server3 = null;
                Thread Client = null;
                Thread Server = null;
                string ClientIdentity = "Client";
                string ServerIdentity = "Server";
                string ClientIdentity2 = "Client2";
                string ServerIdentity2 = "Server2";
                string ClientIdentity3 = "Client3";
                string ServerIdentity3 = "Server3";
                string AddrForSub = "";

                Random random = new Random();

                Console.WriteLine("\nPress number to choose type of clients sockets.");
                Console.WriteLine("1 - REQ-REP");
                Console.WriteLine("2 - PUSH-PULL");
                Console.WriteLine("3 - PUB-SUB");
                Console.WriteLine("4 - All sockets different");
                Console.WriteLine("5 - All sockets REQ-REP");
                Console.WriteLine("6 - All sockets PUSH-PULL");
                Console.WriteLine("7 - All sockets PUB-SUB\n");

                ConsoleKey key2 = Console.ReadKey(true).Key;

                if (key2 == ConsoleKey.D1)
                {
                    socketTypeClient = ZSocketType.REQ;
                    socketTypeServer = ZSocketType.REP;
                    AddressforClient = AddressforClientReq[random.Next(0, AddressforClientReq.Length)];
                    AddressforServer = AddressforServerRep[0];

                    Client = new Thread(() => ClientCreator.Client(context, ClientIdentity, AddressforClient, socketTypeClient, AddressforServer));
                    Server = new Thread(() => ServerCreator.Server(context, ServerIdentity, AddressforServer, socketTypeServer,""));
                }
                else if (key2 == ConsoleKey.D2)
                {
                    socketTypeClient = ZSocketType.PUSH;
                    socketTypeServer = ZSocketType.PULL;
                    AddressforClient = AddressforClientPush[random.Next(0, AddressforClientPush.Length)];
                    AddressforServer = AddressforServerPull[0];

                    Client = new Thread(() => ClientCreator.Client(context, ClientIdentity, AddressforClient, socketTypeClient, AddressforServer));
                    Server = new Thread(() => ServerCreator.Server(context, ServerIdentity, AddressforServer, socketTypeServer,""));
                }
                else if (key2 == ConsoleKey.D3)
                {
                    int randIndex = random.Next(0, AddressforServerSub.Length);
                    
                    socketTypeClient = ZSocketType.PUB;
                    socketTypeServer = ZSocketType.SUB;
                    AddressforClient = AddressforClientPub[random.Next(0, AddressforClientPub.Length)];
                    AddressforServer = AddressforServerSub[randIndex];
                    AddrForSub = AddressforClientReq[randIndex];

                    Client = new Thread(() => ClientCreator.Client(context, ClientIdentity, AddressforClient, socketTypeClient, AddressforServer));
                    Server = new Thread(() => ServerCreator.Server(context, ServerIdentity3, AddressforServer, socketTypeServer, AddrForSub));
                }
                else if (key2 == ConsoleKey.D4)
                {
                    socketTypeClient = ZSocketType.REQ;
                    socketTypeServer = ZSocketType.REP;
                    AddressforClient = AddressforClientReq[0];
                    AddressforServer = AddressforServerRep[1];
                    socketTypeClient2 = ZSocketType.PUSH;
                    socketTypeServer2 = ZSocketType.PULL;
                    AddressforClient2 = AddressforClientPush[1];
                    AddressforServer2 = AddressforServerPull[2];
                    socketTypeClient3 = ZSocketType.PUB;
                    socketTypeServer3 = ZSocketType.SUB;
                    AddressforClient3 = AddressforClientPub[2];
                    AddressforServer3 = AddressforServerSub[0];
                    AddrForSub = AddressforClientReq[0];

                    Client = new Thread(() => ClientCreator.Client(context, ClientIdentity, AddressforClient, socketTypeClient, AddressforServer));
                    Server = new Thread(() => ServerCreator.Server(context, ServerIdentity, AddressforServer, socketTypeServer, ""));
                    Client2 = new Thread(() => ClientCreator.Client(context, ClientIdentity2, AddressforClient2, socketTypeClient2, AddressforServer2));
                    Server2 = new Thread(() => ServerCreator.Server(context, ServerIdentity2, AddressforServer2, socketTypeServer2, ""));
                    Client3 = new Thread(() => ClientCreator.Client(context, ClientIdentity3, AddressforClient3, socketTypeClient3, ""));
                    Server3 = new Thread(() => ServerCreator.Server(context, ServerIdentity3, AddressforServer3, socketTypeServer3, AddrForSub));
                }
                else if (key2 == ConsoleKey.D5)
                {
                    socketTypeClient = ZSocketType.REQ;
                    socketTypeServer = ZSocketType.REP;
                    AddressforClient = AddressforClientReq[0];
                    AddressforServer = AddressforServerRep[1];
                    socketTypeClient2 = ZSocketType.REQ;
                    socketTypeServer2 = ZSocketType.REP;
                    AddressforClient2 = AddressforClientReq[1];
                    AddressforServer2 = AddressforServerRep[2];
                    socketTypeClient3 = ZSocketType.REQ;
                    socketTypeServer3 = ZSocketType.REP;
                    AddressforClient3 = AddressforClientReq[2];
                    AddressforServer3 = AddressforServerRep[0];

                    Client = new Thread(() => ClientCreator.Client(context, ClientIdentity, AddressforClient, socketTypeClient, AddressforServer));
                    Server = new Thread(() => ServerCreator.Server(context, ServerIdentity, AddressforServer, socketTypeServer, ""));
                    Client2 = new Thread(() => ClientCreator.Client(context, ClientIdentity2, AddressforClient2, socketTypeClient2, AddressforServer2));
                    Server2 = new Thread(() => ServerCreator.Server(context, ServerIdentity2, AddressforServer2, socketTypeServer2, ""));
                    Client3 = new Thread(() => ClientCreator.Client(context, ClientIdentity3, AddressforClient3, socketTypeClient3, AddressforServer3));
                    Server3 = new Thread(() => ServerCreator.Server(context, ServerIdentity3, AddressforServer3, socketTypeServer3, ""));
                }
                else if (key2 == ConsoleKey.D6)
                {
                    socketTypeClient = ZSocketType.PUSH;
                    socketTypeServer = ZSocketType.PULL;
                    AddressforClient = AddressforClientPush[0];
                    AddressforServer = AddressforServerPull[1];
                    socketTypeClient2 = ZSocketType.PUSH;
                    socketTypeServer2 = ZSocketType.PULL;
                    AddressforClient2 = AddressforClientPush[1];
                    AddressforServer2 = AddressforServerPull[2];
                    socketTypeClient3 = ZSocketType.PUSH;
                    socketTypeServer3 = ZSocketType.PULL;
                    AddressforClient3 = AddressforClientPush[2];
                    AddressforServer3 = AddressforServerPull[0];

                    Client = new Thread(() => ClientCreator.Client(context, ClientIdentity, AddressforClient, socketTypeClient, AddressforServer));
                    Server = new Thread(() => ServerCreator.Server(context, ServerIdentity, AddressforServer, socketTypeServer, ""));
                    Client2 = new Thread(() => ClientCreator.Client(context, ClientIdentity2, AddressforClient2, socketTypeClient2, AddressforServer2));
                    Server2 = new Thread(() => ServerCreator.Server(context, ServerIdentity2, AddressforServer2, socketTypeServer2, ""));
                    Client3 = new Thread(() => ClientCreator.Client(context, ClientIdentity3, AddressforClient3, socketTypeClient3, AddressforServer3));
                    Server3 = new Thread(() => ServerCreator.Server(context, ServerIdentity3, AddressforServer3, socketTypeServer3, ""));
                }
                else if (key2 == ConsoleKey.D7)
                {
                    socketTypeClient = ZSocketType.PUB;
                    socketTypeServer = ZSocketType.SUB;
                    AddressforClient = AddressforClientPub[0];
                    AddressforServer = AddressforServerSub[1];
                    socketTypeClient2 = ZSocketType.PUB;
                    socketTypeServer2 = ZSocketType.SUB;
                    AddressforClient2 = AddressforClientPub[1];
                    AddressforServer2 = AddressforServerSub[2];
                    socketTypeClient3 = ZSocketType.PUB;
                    socketTypeServer3 = ZSocketType.SUB;
                    AddressforClient3 = AddressforClientPub[2];
                    AddressforServer3 = AddressforServerSub[0];
                    AddrForSub = AddressforClientReq[1];
                    string AddrForSub2 = AddressforClientReq[2];
                    string AddrForSub3 = AddressforClientReq[0];

                    Client = new Thread(() => ClientCreator.Client(context, ClientIdentity, AddressforClient, socketTypeClient, ""));
                    Server = new Thread(() => ServerCreator.Server(context, ServerIdentity, AddressforServer, socketTypeServer, AddrForSub));
                    Client2 = new Thread(() => ClientCreator.Client(context, ClientIdentity2, AddressforClient2, socketTypeClient2, ""));
                    Server2 = new Thread(() => ServerCreator.Server(context, ServerIdentity2, AddressforServer2, socketTypeServer2, AddrForSub2));
                    Client3 = new Thread(() => ClientCreator.Client(context, ClientIdentity3, AddressforClient3, socketTypeClient3, ""));
                    Server3 = new Thread(() => ServerCreator.Server(context, ServerIdentity3, AddressforServer3, socketTypeServer3, AddrForSub3));
                }

                Console.WriteLine("Press number to choose test. Press S to stop.");
                Console.WriteLine("1 - all components active");
                if (key2 == ConsoleKey.D1 || key2 == ConsoleKey.D2 || key2 == ConsoleKey.D3)
                {
                    Console.WriteLine("2 - passive sending client after sending and active receiving client after 5 sec\n");
                }
                else Console.WriteLine("\n");

                ConsoleKey key = Console.ReadKey(true).Key;

                broker1.Start();
                broker2.Start();
                broker3.Start();

                if (key == ConsoleKey.D1)
                {
                    Server.Start(); 
                    Client.Start();

                    if (AddressforClient2 != "")
                    {
                        Server2.Start(); 
                        Server3.Start();
                        Client2.Start();
                        Client3.Start();
                    }
                }
                else if (key == ConsoleKey.D2)
                {
                    Client.Start();
                    Thread.Sleep(10 * 1000);
                    Client.Abort();
                    Thread.Sleep(5 * 1000);
                    Server.Start();
                    Thread.Sleep(5 * 1000);
                    Client = new Thread(() => ClientCreator.Client(context, ClientIdentity, AddressforClient, socketTypeClient, AddressforServer));
                    Client.Start();
                }

                while (Console.ReadKey(true).Key != ConsoleKey.S)
                {
                    Thread.Sleep(1);
                }
                Client.Abort();
                Server.Abort();
                if (Client2 != null)
                {
                    Client2.Abort();
                    Server2.Abort();
                    Client3.Abort();
                    Server3.Abort();
                }
                broker1.Abort();
                broker2.Abort();
                broker3.Abort();
            }
        }
	}
}