using System;
using System.Text;
using ZeroMQ;
using System.Threading;

namespace PersistentCommunicationZMQ
{
    public abstract class ClientCreator:SendCreator
    {
        static ZSocket CreateZSocket(ZContext context, string identity, string address, ZSocketType socketType)
        {
            var client = new ZSocket(context, socketType);
            client.Identity = Encoding.UTF8.GetBytes(identity);
            //client.Linger = TimeSpan.FromMilliseconds(0);
            client.Connect(address);
            return client;
        }
        public static void Client(ZContext context, string identity, string ProxyAddressforClient, ZSocketType socketType, string receiverAddress)
        {           
            var client = CreateZSocket(context, identity, ProxyAddressforClient, socketType);

            string text = "Echo";
            ZError error;
            ZMessage request;
            ZPollItem poll = ZPollItem.CreateReceiver();
            int counter = 0;

            try
            {
                if (socketType == ZSocketType.REQ)
                {
                    Send(client, client.Identity, text, receiverAddress);

                    while (true)
                    {                        
                        if (client.PollIn(poll, out request, out error, TimeSpan.FromMilliseconds(20 * 1000)))
                        {
                            string requestID = request[0].ReadString(); 
                            string receivedIdentity = request[1].ReadString(); 

                            Console.WriteLine("{0} <- {1} : [{2} {3}]", identity, receivedIdentity, requestID, "REP");
                            break;
                        }
                        else
                        {
                            if (error == ZError.EAGAIN)
                            {
                                client.Dispose();
                                client = CreateZSocket(context, identity, ProxyAddressforClient, socketType);
                                Send(client, client.Identity, text, receiverAddress);
                                continue;
                            }
                            if (error == ZError.ETERM)
                                return;    // Interrupted
                            throw new ZException(error);
                        }
                    }
                }
                else if (socketType == ZSocketType.PUSH)
                {
                    while (counter < 3)
                    {
                        bool sended = Send(client, client.Identity, text, receiverAddress);
                        if (sended == false)
                            counter--; 
                        Thread.Sleep(3 * 1000);
                        counter++;
                    }
                }
                else if (socketType == ZSocketType.PUB)
                {
                    string topic = null;
                    
                    if (identity == "Client") topic = "Topic";
                    if (identity == "Client2") topic = "A";
                    if (identity == "Client3") topic = "B";

                    while (counter < 3)
                    {
                        Thread.Sleep(3 * 1000); 
                        bool sended = Send(client, client.Identity, text, topic);
                        if (sended == false)
                            counter--;
                        counter++;
                    }
                }
            }
            finally
            {
                if (client != null)
                    client.Dispose();
            }
        }
    }
}
