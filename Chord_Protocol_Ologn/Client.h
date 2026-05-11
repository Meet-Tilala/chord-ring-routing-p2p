#ifndef __CHORD_CLIENT_H_
#define __CHORD_CLIENT_H_

#include <omnetpp.h>

#include <fstream>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "ChordMessages_m.h"

using namespace omnetpp;

class ChordClient : public cSimpleModule
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
    std::vector<int> fingerTable;
    std::map<int, int> subtaskResults;
    std::set<std::string> seenGossipHashes;

    bool taskCompleted;
    bool gossipSent;
    bool allGossipReceived;

    std::ofstream outputFile;
    std::ofstream sharedOutputFile;

    void loadTopology();
    void buildFingerTable();
    void initializeTaskData();
    void startLocalTask();
    void processTask(ChordTaskMessage *msg);
    void handleResult(ChordResultMessage *msg);
    void handleGossip(ChordGossipMessage *msg);
    void sendResult(int taskOwnerId, int subtaskId, int result);
    void forwardTask(ChordTaskMessage *msg);
    void forwardResult(ChordResultMessage *msg);
    void broadcastGossip();
    void markNodeFinished();

    int determineSubtaskCount() const;
    int clockwiseDistance(int fromId, int toId) const;
    int getSuccessor() const;
    int getNextHop(int destinationId) const;
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
