message RingTaskMessage
{
    int taskOwnerId;
    int subtaskId;
    int destinationClientId;
    int lastHopId = -1;
    int hopCount = 0;
    int data[];
}

message RingResultMessage
{
    int taskOwnerId;
    int subtaskId;
    int processorClientId;
    int destinationClientId;
    int lastHopId = -1;
    int hopCount = 0;
    int result;
}

message RingGossipMessage
{
    string content;
    string originIp;
    int originClientId;
    string messageHash;
    int lastHopId = -1;
}
