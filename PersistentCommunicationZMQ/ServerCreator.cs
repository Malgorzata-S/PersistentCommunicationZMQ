using System;
using System.Text;
using ZeroMQ;

namespace PersistentCommunicationZMQ
{
    public abstract class ServerCreator:SendCreator
    {
        public static void Server(ZContext context, String identity, string BrokerAddressforServer, ZSocketType socketType, string AddrForSub)
        {
            using (var server = new ZSocket(context, socketType))
            {
                server.Connect(BrokerAddressforServer);
                server.Identity = Encoding.UTF8.GetBytes(identity);

                string text = "Confirmed Echo";
                ZPollItem poll = ZPollItem.CreateReceiver();
                ZError error;
                ZMessage request2;

                try
                {
                    if (socketType == ZSocketType.REP)
                    {
                        using (ZMessage request = server.ReceiveMessage())
                        {
                            string uid = request[0].ReadString(); 
                            string receiverIdentity = request[1].ReadString();

                            Console.WriteLine("{0} <- {1} : [{2} {3}]", identity, receiverIdentity, uid, "REQ");

                            Send(server, Encoding.UTF8.GetBytes(identity), text, uid);
                        }
                    }
                    else if (socketType == ZSocketType.PULL)
                    {
                        while (true)
                        {
                            if (server.PollIn(poll, out request2, out error, TimeSpan.FromMilliseconds(1000)))
                            {
                                string uid = request2[1].ReadString();
                                string receivedIdentity = request2[2].ReadString();

                                Console.WriteLine("{0} <- {1} : [{2}]", identity, receivedIdentity, uid);
                            }
                            else
                            {
                                ErrorChecker(error);
                            }
                        }
                    }
                    else if (socketType == ZSocketType.SUB)
                    {
                        if (identity == "Server") server.Subscribe("B");
                        if (identity == "Server2") server.Subscribe("Topic");
                        if (identity == "Server3") server.SubscribeAll();

                        while (true)
                        {
                            using (ZMessage request = server.ReceiveMessage())
                            {
                                string topic = request[0].ReadString();
                                string uid = request[1].ReadString();
                                string receivedIdentity = request[2].ReadString();

                                Console.WriteLine("{0} <- {1} : [{2} {3}]", identity, receivedIdentity, uid, topic);
                            }
                        }
                    }
                }
                catch (ZException)
                {
                    context = new ZContext();
                }
            }
        }
    }
} 
