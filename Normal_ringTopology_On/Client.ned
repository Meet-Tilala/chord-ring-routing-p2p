simple RingClient
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
