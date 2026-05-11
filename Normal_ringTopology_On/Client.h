#ifndef __RING_CLIENT_H_
#define __RING_CLIENT_H_

#include <omnetpp.h>

#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "messages_m.h"

using namespace omnetpp;

class RingClient : public cSimpleModule
{
  private:
    static std::set<int> finishedNodes;

    int clientId;
    int numClients;
    int arraySize;
    int requestedSubtasks;
    int actualSubtasks;

    std::string topologyFile;
    std::vector<int> dataArray;
    std::vector<int> neighborIds;
    std::map<int, std::vector<int>> adjacency;
    std::map<int, int> subtaskResults;
    std::set<std::string> seenGossipHashes;

    bool taskCompleted;
    bool gossipSent;
    bool allGossipReceived;

    std::ofstream outputFile;
    std::ofstream sharedOutputFile;

    void loadTopology();
    void initializeTaskData();
    void startLocalTask();
    void processTask(RingTaskMessage *msg);
    void handleResult(RingResultMessage *msg);
    void handleGossip(RingGossipMessage *msg);
    void sendResult(int taskOwnerId, int subtaskId, int result);
    void forwardTask(RingTaskMessage *msg);
    void forwardResult(RingResultMessage *msg);
    void broadcastGossip();
    void markNodeFinished();

    int determineSubtaskCount() const;
    int getNextHop(int destinationId) const;
    int getSuccessorHop(int currentId, const std::map<int, int>& parent) const;
    std::string getClientIp(int id) const;
    std::string getTimestamp() const;
    std::string hashMessage(const std::string& message) const;
    void logLine(const std::string& line);
    cModule *getClientModule(int id) const;

  protected:
    void initialize() override;
    void handleMessage(cMessage *msg) override;
    void finish() override;
};

#endif
