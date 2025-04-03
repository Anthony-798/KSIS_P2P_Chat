using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Лаб_3_КСиС {
    internal class Program {
        class PeerInfo {
            public TcpClient Client { get; set; }
            public string Name { get; set; }
            public string Ip { get; set; }
            public bool IsInitialized { get; set; }
            public DateTime ConnectionTime { get; set; }
        }

        static int udpPort = 5000;
        static int tcpPort = 5001;

        static UdpClient udpClient;
        static TcpListener tcpListener;

        static List<PeerInfo> peers = new List<PeerInfo>();
        static List<string> history = new List<string>();
        static string userName;
        static bool historyRequested = false;

        static void Main(string[] args) {
            Console.Write("\nВведите IP-адрес для подключения к чату: ");
            string ipAddress = Console.ReadLine();
            Console.Write("Введите ваше имя: ");
            userName = Console.ReadLine();
            Console.Write("\n");
            IPAddress localAddr = IPAddress.Parse(ipAddress);
            udpClient = new UdpClient(new IPEndPoint(localAddr, udpPort));

            if (!IsPortAvailable(tcpPort)) {
                Console.WriteLine($"Порт {tcpPort} уже используется. Выберите другой порт.");
                return;
            }

            tcpListener = new TcpListener(localAddr, tcpPort);

            Thread udpThread = new Thread(new ThreadStart(ReceiveUdpBroadcasts));
            udpThread.Start();

            Thread tcpThread = new Thread(new ThreadStart(AcceptTcpClients));
            tcpThread.Start();

            SendUdpBroadcast();

            while (true) {
                foreach (var peer in peers.ToArray()) {
                    if (!IsConnected(peer.Client)) {
                        HandleClientDisconnection(peer.Client);
                    }
                }
                string message = Console.ReadLine();
                SendMessageToPeers(message);
            }
        }

        static bool IsConnected(TcpClient client) {
            try {
                if (client == null || !client.Connected) return false;
                return !(client.Client.Poll(1, SelectMode.SelectRead) && client.Available == 0);
            }
            catch {
                return false;
            }
        }

        static bool IsPortAvailable(int port) {
            bool available = true;
            try {
                TcpListener listener = new TcpListener(IPAddress.Any, port);
                listener.Start();
                listener.Stop();
            }
            catch (SocketException) {
                available = false;
            }
            return available;
        }

        static void SendUdpBroadcast() {
            byte[] nameData = Encoding.UTF8.GetBytes(userName);
            using (MemoryStream ms = new MemoryStream()) {
                ms.WriteByte(2);
                byte[] lengthBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(nameData.Length));
                ms.Write(lengthBytes, 0, 4);
                ms.Write(nameData, 0, nameData.Length);
                byte[] packet = ms.ToArray();
                udpClient.Send(packet, packet.Length, new IPEndPoint(IPAddress.Broadcast, udpPort));
            }
        }

        static void ReceiveUdpBroadcasts() {
            IPEndPoint remoteEndPoint = new IPEndPoint(IPAddress.Any, udpPort);
            while (true) {
                byte[] data = udpClient.Receive(ref remoteEndPoint);
                if (data.Length < 5) continue;

                using (MemoryStream ms = new MemoryStream(data))
                using (BinaryReader reader = new BinaryReader(ms)) {
                    byte type = reader.ReadByte();
                    int length = IPAddress.NetworkToHostOrder(reader.ReadInt32());
                    if (length != ms.Length - 5) continue;

                    string receivedName = Encoding.UTF8.GetString(reader.ReadBytes(length));
                    if (type == 2 && receivedName != userName) {
                        string ip = remoteEndPoint.Address.ToString();
                        string localMessage = $"{DateTime.Now:HH:mm:ss} Установлено соединение с узлом {receivedName} ({ip})";
                        string historyMessage = $"{DateTime.Now:HH:mm:ss} Узел {userName} ({((IPEndPoint)udpClient.Client.LocalEndPoint).Address}) установил соединение с узлом {receivedName} ({ip})";
                        Console.WriteLine("\n" + localMessage + "\n");
                        if (!history.Contains(historyMessage)) {
                            history.Add(historyMessage);
                        }

                        bool exists = peers.Any(p => p.Ip == ip && p.Client.Connected);
                        if (!exists) ConnectToPeer(ip);
                    }
                }
            }
        }

        static void ConnectToPeer(string ipAddress) {
            try {
                TcpClient client = new TcpClient();
                client.Connect(ipAddress, tcpPort);

                string identityData = $"{userName}|{((IPEndPoint)udpClient.Client.LocalEndPoint).Address}";
                byte[] data = Encoding.UTF8.GetBytes(identityData);
                using (MemoryStream ms = new MemoryStream()) {
                    ms.WriteByte(2);
                    byte[] lengthBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(data.Length));
                    ms.Write(lengthBytes, 0, 4);
                    ms.Write(data, 0, data.Length);
                    client.GetStream().Write(ms.ToArray(), 0, (int)ms.Length);
                }

                peers.Add(new PeerInfo { Client = client, Ip = ipAddress, IsInitialized = false, ConnectionTime = DateTime.Now });
                Thread receiveThread = new Thread(() => ReceiveMessages(client));
                receiveThread.Start();
            }
            catch { }
        }

        static void AcceptTcpClients() {
            tcpListener.Start();
            while (true) {
                TcpClient client = tcpListener.AcceptTcpClient();
                peers.Add(new PeerInfo {
                    Client = client,
                    Ip = ((IPEndPoint)client.Client.RemoteEndPoint).Address.ToString(),
                    IsInitialized = false,
                    ConnectionTime = DateTime.Now
                });
                Thread receiveThread = new Thread(() => ReceiveMessages(client));
                receiveThread.Start();
            }
        }

        static void ReceiveMessages(TcpClient client) {
            NetworkStream stream = client.GetStream();
            byte[] headerBuffer = new byte[5];
            bool historyReceived = false;
            List<string> historyBuffer = new List<string>();

            try {
                while (true) {
                    int bytesRead = stream.Read(headerBuffer, 0, 5);
                    if (bytesRead == 0) break;

                    byte type = headerBuffer[0];
                    int length = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(headerBuffer, 1));

                    byte[] dataBuffer = new byte[length];
                    int totalRead = 0;
                    while (totalRead < length) {
                        bytesRead = stream.Read(dataBuffer, totalRead, length - totalRead);
                        if (bytesRead == 0) throw new IOException();
                        totalRead += bytesRead;
                    }

                    switch (type) {
                        case 1: // Текстовое сообщение
                            string message = Encoding.UTF8.GetString(dataBuffer);
                            string[] parts = message.Split(':');
                            if (parts[0] != userName) {
                                string logMessage = $"{DateTime.Now:HH:mm:ss} {message}";
                                if (!history.Contains(logMessage)) {
                                    history.Add(logMessage);
                                }
                                Console.WriteLine("\n" + logMessage + "\n");
                            }
                            break;

                        case 2: // Новый узел
                            string[] connectParts = Encoding.UTF8.GetString(dataBuffer).Split('|');
                            if (connectParts.Length >= 2) {
                                string peerName = connectParts[0];
                                string peerIp = connectParts[1];
                                var peer = peers.FirstOrDefault(p => p.Client == client);
                                if (peer != null) {
                                    peer.Name = peerName;
                                    peer.Ip = peerIp;
                                    peer.IsInitialized = true;
                                    string localMessage1 = $"{DateTime.Now:HH:mm:ss} Установлено соединение с узлом {peerName} ({peerIp})";
                                    string historyMessage1 = $"{DateTime.Now:HH:mm:ss} Узел {userName} ({((IPEndPoint)udpClient.Client.LocalEndPoint).Address}) установил соединение с узлом {peerName} ({peerIp})";
                                    Console.WriteLine("\n" + localMessage1 + "\n");
                                    if (!history.Contains(historyMessage1)) {
                                        history.Add(historyMessage1);
                                    }

                                    var firstPeer = peers.Where(p => p.Client.Connected).OrderBy(p => p.ConnectionTime).FirstOrDefault();
                                    if (!historyRequested && firstPeer == peer) {
                                        RequestHistoryFromPeer(client);
                                        historyRequested = true;
                                    }
                                }
                            }
                            break;

                        case 3: // Запрос истории
                            SendHistory(client);
                            break;

                        case 4: // Отключение узла
                            string[] disconnectParts = Encoding.UTF8.GetString(dataBuffer).Split('|');
                            string disconnectedName = disconnectParts.Length > 0 ? disconnectParts[0] : "Неизвестный узел";
                            string disconnectedIp = disconnectParts.Length > 1 ? disconnectParts[1] : ((IPEndPoint)client.Client.RemoteEndPoint).Address.ToString();
                            var knownPeer = peers.FirstOrDefault(p => p.Ip == disconnectedIp);
                            if (knownPeer != null) {
                                disconnectedName = knownPeer.Name;
                                peers.Remove(knownPeer);
                            }
                            string localMessage = $"{DateTime.Now:HH:mm:ss} Соединение с узлом {disconnectedIp} разорвано.";
                            string historyMessage = $"{DateTime.Now:HH:mm:ss} Узел {userName} ({((IPEndPoint)udpClient.Client.LocalEndPoint).Address}) разорвал соединение с узлом {disconnectedName} ({disconnectedIp})";
                            Console.WriteLine("\n" + localMessage + "\n");
                            if (!history.Contains(historyMessage)) {
                                history.Add(historyMessage);
                            }
                            break;

                        case 5: // Получение истории
                            string historyEntry = Encoding.UTF8.GetString(dataBuffer);
                            string localIp = ((IPEndPoint)udpClient.Client.LocalEndPoint).Address.ToString();
                            // Не добавляем в историю запись, если текущий узел - цель подключения
                            if (!historyEntry.Contains($"установил соединение с узлом {userName} ({localIp})") && !history.Contains(historyEntry)) {
                                history.Add(historyEntry);
                            }
                            // Обновляем буфер для вывода
                            historyBuffer.Clear();
                            // Фильтруем локальные подключения при выводе
                            foreach (var entry in history) {
                                if (!entry.Contains($"{userName} ({localIp}) установил соединение с узлом")) {
                                    historyBuffer.Add(entry);
                                }
                            }
                            Thread.Sleep(50);
                            if (!historyReceived && !stream.DataAvailable) {
                                historyBuffer.Sort((a, b) => {
                                    string timeA = a.Substring(0, 8);
                                    string timeB = b.Substring(0, 8);
                                    return DateTime.Parse(timeA).CompareTo(DateTime.Parse(timeB));
                                });
                                Console.WriteLine("\nИстория событий:\n" + String.Join("\n", historyBuffer) + "\n");
                                historyReceived = true;
                            }
                            break;
                    }
                }
            }
            catch {
                HandleClientDisconnection(client);
            }
        }

        static void SendMessageToPeers(string message) {
            string fullMessage = $"{userName}: {message}";
            byte[] messageData = Encoding.UTF8.GetBytes(fullMessage);
            using (MemoryStream ms = new MemoryStream()) {
                ms.WriteByte(1);
                byte[] lengthBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(messageData.Length));
                ms.Write(lengthBytes, 0, 4);
                ms.Write(messageData, 0, messageData.Length);
                byte[] packet = ms.ToArray();
                foreach (var peerInfo in peers.ToArray()) {
                    try {
                        var peerIp = ((IPEndPoint)peerInfo.Client.Client.RemoteEndPoint).Address;
                        var localIp = ((IPEndPoint)peerInfo.Client.Client.LocalEndPoint).Address;
                        if (!peerIp.Equals(localIp) && peerInfo.Client.Connected) {
                            peerInfo.Client.GetStream().Write(packet, 0, packet.Length);
                        }
                    }
                    catch { }
                }
            }
            string logMessage = $"{DateTime.Now:HH:mm:ss} {userName}: {message}";
            if (!history.Contains(logMessage)) {
                history.Add(logMessage);
            }
            Console.WriteLine("\n" + logMessage + "\n");
        }

        static void HandleClientDisconnection(TcpClient client) {
            try {
                var peer = peers.FirstOrDefault(p => p.Client == client);
                if (peer == null) return;

                if (!peer.IsInitialized) {
                    peer.Ip = ((IPEndPoint)client.Client.RemoteEndPoint).Address.ToString();
                    peer.Name = "Неизвестный узел";
                }

                peers.Remove(peer);

                string localMessage = $"{DateTime.Now:HH:mm:ss} Соединение с узлом {peer.Ip} разорвано.";
                string historyMessage = $"{DateTime.Now:HH:mm:ss} Узел {userName} ({((IPEndPoint)udpClient.Client.LocalEndPoint).Address}) разорвал соединение с узлом {peer.Name} ({peer.Ip})";
                Console.WriteLine("\n" + localMessage + "\n");
                if (!history.Contains(historyMessage)) {
                    history.Add(historyMessage);
                }

                if (client.Connected) {
                    string disconnectData = $"{peer.Name}|{peer.Ip}";
                    byte[] data = Encoding.UTF8.GetBytes(disconnectData);
                    using (MemoryStream ms = new MemoryStream()) {
                        ms.WriteByte(4);
                        byte[] lengthBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(data.Length));
                        ms.Write(lengthBytes, 0, 4);
                        ms.Write(data, 0, data.Length);
                        foreach (var remainingPeer in peers.ToArray()) {
                            try {
                                if (remainingPeer.Client.Connected) {
                                    remainingPeer.Client.GetStream().Write(ms.ToArray(), 0, (int)ms.Length);
                                }
                            }
                            catch { }
                        }
                    }
                }
            }
            finally {
                client.Close();
            }
        }

        static void RequestHistoryFromPeer(TcpClient client) {
            using (MemoryStream ms = new MemoryStream()) {
                ms.WriteByte(3);
                byte[] lengthBytes = BitConverter.GetBytes(0);
                ms.Write(lengthBytes, 0, 4);
                client.GetStream().Write(ms.ToArray(), 0, 5);
            }
        }

        static void SendHistory(TcpClient client) {
            foreach (var entry in history) {
                byte[] data = Encoding.UTF8.GetBytes(entry);
                using (MemoryStream ms = new MemoryStream()) {
                    ms.WriteByte(5);
                    byte[] lengthBytes = BitConverter.GetBytes(IPAddress.HostToNetworkOrder(data.Length));
                    ms.Write(lengthBytes, 0, 4);
                    ms.Write(data, 0, data.Length);
                    client.GetStream().Write(ms.ToArray(), 0, (int)ms.Length);
                }
            }
        }
    }
}