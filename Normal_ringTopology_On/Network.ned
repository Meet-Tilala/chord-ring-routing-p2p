network RingNetwork
{
    parameters:
        int N = default(8);
        int K = default(64);
        int X = default(16);
        string topoFile = default("topology.txt");

    submodules:
        client[N]: RingClient {
            parameters:
                clientId = index;
                numClients = parent.N;
                arraySize = parent.K;
                numSubtasks = parent.X;
                topologyFile = parent.topoFile;
        }

    connections allowunconnected:
}
