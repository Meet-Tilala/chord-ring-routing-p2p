simple ChordClient
{
    parameters:
        int clientId;
        int numClients;
        int arraySize;
        int numSubtasks;
        string topologyFile;
        @display("i=device/pc");
    gates:
        input directIn @directIn;
}

network ChordNetwork
{
    parameters:
        int N = default(8);
        int K = default(64);
        int X = default(16);
        string topoFile = default("topology.txt");

    submodules:
        client[N]: ChordClient {
            parameters:
                clientId = index;
                numClients = parent.N;
                arraySize = parent.K;
                numSubtasks = parent.X;
                topologyFile = parent.topoFile;
        }

    connections allowunconnected:
}
