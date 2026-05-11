message ChordTaskMessage
{
    int taskOwnerId;
    int subtaskId;
    int destinationClientId;
    int lastHopId = -1;
    int hopCount = 0;
    int data[];
}

message ChordResultMessage
{
    int taskOwnerId;
    int subtaskId;
    int processorClientId;
    int destinationClientId;
    int lastHopId = -1;
    int hopCount = 0;
    int result;
}

message ChordGossipMessage
{
    string content;
    string originIp;
    int originClientId;
    string messageHash;
    int lastHopId = -1;
}
