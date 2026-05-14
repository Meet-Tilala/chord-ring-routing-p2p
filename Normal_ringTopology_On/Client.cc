#include "Client.h"
#include <time>
#include <algorithm>
#include <functional>
#include <limits>
#include <queue>
#include <sstream>
#include <stdexcept>

Define_Module(RingClient);

std::set<int> RingClient::finishedNodes;

void RingClient::initialize()
{
    clientId = par("clientId");
    numClients = par("numClients");
    arraySize = par("arraySize");
    requestedSubtasks = par("numSubtasks");
    topologyFile = par("topologyFile").stringValue();

    if (clientId == 0) {
        finishedNodes.clear();
    }

    taskCompleted = false;
    gossipSent = false;
    allGossipReceived = false;
    actualSubtasks = 0;

    std::ostringstream filename;
    filename << "outputfile_client" << clientId << ".txt";
    outputFile.open(filename.str().c_str(), std::ios::out);
    if (clientId == 0) {
        std::ofstream resetFile("outputfile.txt", std::ios::out | std::ios::trunc);
    }
    sharedOutputFile.open("outputfile.txt", std::ios::out | std::ios::app);

    loadTopology();
    initializeTaskData();

    logLine("Client " + std::to_string(clientId) + " initialized with "
            + std::to_string(neighborIds.size()) + " ring neighbors.");

    cMessage *startMessage = new cMessage("startLocalTask");
    scheduleAt(simTime() + SimTime(0.1 + (0.01 * clientId)), startMessage);
}

void RingClient::loadTopology()
{
    std::ifstream input(topologyFile.c_str());
    if (!input.is_open()) {
        throw cRuntimeError("Unable to open topology file '%s'", topologyFile.c_str());
    }

    std::string line;
    while (std::getline(input, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }

        std::replace(line.begin(), line.end(), ':', ' ');
        std::replace(line.begin(), line.end(), ',', ' ');

        std::istringstream parser(line);
        int nodeId = -1;
        parser >> nodeId;
        if (nodeId < 0) {
            continue;
        }

        int neighbor = -1;
        std::vector<int> neighbors;
        while (parser >> neighbor) {
            if (neighbor >= 0 && neighbor < numClients && neighbor != nodeId) {
                if (std::find(neighbors.begin(), neighbors.end(), neighbor) == neighbors.end()) {
                    neighbors.push_back(neighbor);
                }
            }
        }

        adjacency[nodeId] = neighbors;
    }

    auto found = adjacency.find(clientId);
    if (found == adjacency.end() || found->second.empty()) {
        throw cRuntimeError("Client %d has no neighbors configured in '%s'",
                            clientId, topologyFile.c_str());
    }

    neighborIds = found->second;
}

void RingClient::initializeTaskData()
{
    dataArray.clear();
    dataArray.reserve(arraySize);
    for (int index = 0; index < arraySize; ++index) {
        dataArray.push_back(intrand(1000) + 1);
    }

    std::ostringstream summary;
    summary << "Task data generated for client " << clientId << " (first values:";
    const int preview = std::min(arraySize, 8);
    for (int index = 0; index < preview; ++index) {
        summary << ' ' << dataArray[index];
    }
    if (arraySize > preview) {
        summary << " ...";
    }
    summary << ")";
    logLine(summary.str());
}

int RingClient::determineSubtaskCount() const
{
    if (arraySize < 2) {
        return 1;
    }

    int maxSubtasks = std::max(1, arraySize / 2);
    return std::max(1, std::min(requestedSubtasks, maxSubtasks));
}

void RingClient::startLocalTask()
{
    actualSubtasks = determineSubtaskCount();
    subtaskResults.clear();

    if (requestedSubtasks != actualSubtasks) {
        logLine("Adjusted subtask count for client " + std::to_string(clientId)
                + " from " + std::to_string(requestedSubtasks) + " to "
                + std::to_string(actualSubtasks) + " to satisfy K/X >= 2.");
    }

    if (actualSubtasks <= numClients) {
        logLine("Warning: X <= N for client " + std::to_string(clientId)
                + ". The assignment expects X > N.");
    }

    const int baseChunkSize = arraySize / actualSubtasks;
    const int remainder = arraySize % actualSubtasks;
    int cursor = 0;

    logLine("Client " + std::to_string(clientId) + " starting task with "
            + std::to_string(actualSubtasks) + " subtasks.");

    for (int subtaskId = 0; subtaskId < actualSubtasks; ++subtaskId) {
        const int chunkSize = baseChunkSize + (subtaskId < remainder ? 1 : 0);
        const int destinationId = subtaskId % numClients;

        RingTaskMessage *taskMessage = new RingTaskMessage("ringTask");
        taskMessage->setTaskOwnerId(clientId);
        taskMessage->setSubtaskId(subtaskId);
        taskMessage->setDestinationClientId(destinationId);
        taskMessage->setLastHopId(clientId);
        taskMessage->setHopCount(0);
        taskMessage->setDataArraySize(chunkSize);

        for (int offset = 0; offset < chunkSize; ++offset) {
            taskMessage->setData(offset, dataArray[cursor + offset]);
        }
        cursor += chunkSize;

        if (destinationId == clientId) {
            processTask(taskMessage);
            continue;
        }

        const int nextHop = getNextHop(destinationId);
        taskMessage->setHopCount(taskMessage->getHopCount() + 1);
        taskMessage->setLastHopId(clientId);

        logLine("Dispatching subtask " + std::to_string(subtaskId) + " from client "
                + std::to_string(clientId) + " to client " + std::to_string(destinationId)
                + " via ring neighbor " + std::to_string(nextHop) + ".");

        sendDirect(taskMessage, getClientModule(nextHop), "directIn");
    }
}

void RingClient::processTask(RingTaskMessage *msg)
{
    int localMax = std::numeric_limits<int>::min();
    for (size_t index = 0; index < msg->getDataArraySize(); ++index) {
        localMax = std::max(localMax, msg->getData(index));
    }

    std::ostringstream line;
    line << "Client " << clientId << " processed subtask " << msg->getSubtaskId()
         << " for task owner " << msg->getTaskOwnerId()
         << " and computed max = " << localMax
         << " after " << msg->getHopCount() << " hop(s).";
    logLine(line.str());

    const int taskOwnerId = msg->getTaskOwnerId();
    const int subtaskId = msg->getSubtaskId();
    delete msg;

    sendResult(taskOwnerId, subtaskId, localMax);
}

void RingClient::sendResult(int taskOwnerId, int subtaskId, int result)
{
    if (taskOwnerId == clientId) {
        subtaskResults[subtaskId] = result;
        logLine("Client " + std::to_string(clientId) + " received local result for subtask "
                + std::to_string(subtaskId) + ": " + std::to_string(result) + ".");

        if (!taskCompleted && static_cast<int>(subtaskResults.size()) == actualSubtasks) {
            int finalResult = std::numeric_limits<int>::min();
            for (const auto& entry : subtaskResults) {
                finalResult = std::max(finalResult, entry.second);
            }

            taskCompleted = true;
            logLine("Client " + std::to_string(clientId)
                    + " consolidated final result for its task: "
                    + std::to_string(finalResult) + ".");
            broadcastGossip();
        }
        return;
    }

    RingResultMessage *resultMessage = new RingResultMessage("ringResult");
    resultMessage->setTaskOwnerId(taskOwnerId);
    resultMessage->setSubtaskId(subtaskId);
    resultMessage->setProcessorClientId(clientId);
    resultMessage->setDestinationClientId(taskOwnerId);
    resultMessage->setLastHopId(clientId);
    resultMessage->setHopCount(1);
    resultMessage->setResult(result);

    const int nextHop = getNextHop(taskOwnerId);
    sendDirect(resultMessage, getClientModule(nextHop), "directIn");
}

void RingClient::forwardTask(RingTaskMessage *msg)
{
    const int nextHop = getNextHop(msg->getDestinationClientId());
    msg->setHopCount(msg->getHopCount() + 1);
    msg->setLastHopId(clientId);
    sendDirect(msg, getClientModule(nextHop), "directIn");
}

void RingClient::forwardResult(RingResultMessage *msg)
{
    const int nextHop = getNextHop(msg->getDestinationClientId());
    msg->setHopCount(msg->getHopCount() + 1);
    msg->setLastHopId(clientId);
    sendDirect(msg, getClientModule(nextHop), "directIn");
}

void RingClient::handleResult(RingResultMessage *msg)
{
    std::ostringstream line;
    line << "Client " << clientId << " received subtask " << msg->getSubtaskId()
         << " result " << msg->getResult() << " from client "
         << msg->getProcessorClientId() << " after "
         << msg->getHopCount() << " hop(s).";
    logLine(line.str());

    subtaskResults[msg->getSubtaskId()] = msg->getResult();
    delete msg;

    if (!taskCompleted && static_cast<int>(subtaskResults.size()) == actualSubtasks) {
        int finalResult = std::numeric_limits<int>::min();
        for (const auto& entry : subtaskResults) {
            finalResult = std::max(finalResult, entry.second);
        }

        taskCompleted = true;
        logLine("Client " + std::to_string(clientId)
                + " consolidated final result for its task: "
                + std::to_string(finalResult) + ".");
        broadcastGossip();
    }
}

void RingClient::broadcastGossip()
{
    if (gossipSent) {
        return;
    }

    const std::string timestamp = getTimestamp();
    const std::string ipAddress = getClientIp(clientId);
    const std::string content = timestamp + ":" + ipAddress + ":" + std::to_string(clientId);
    const std::string messageHash = hashMessage(content);

    gossipSent = true;
    seenGossipHashes.insert(messageHash);

    logLine("Client " + std::to_string(clientId) + " generated gossip " + content + ".");

    for (int neighborId : neighborIds) {
        RingGossipMessage *gossipMessage = new RingGossipMessage("ringGossip");
        gossipMessage->setContent(content.c_str());
        gossipMessage->setOriginIp(ipAddress.c_str());
        gossipMessage->setOriginClientId(clientId);
        gossipMessage->setMessageHash(messageHash.c_str());
        gossipMessage->setLastHopId(clientId);
        sendDirect(gossipMessage, getClientModule(neighborId), "directIn");
    }

    if (static_cast<int>(seenGossipHashes.size()) == numClients) {
        markNodeFinished();
    }
}

void RingClient::handleGossip(RingGossipMessage *msg)
{
    const std::string messageHash = msg->getMessageHash();
    if (seenGossipHashes.find(messageHash) != seenGossipHashes.end()) {
        delete msg;
        return;
    }

    seenGossipHashes.insert(messageHash);

    std::ostringstream line;
    line << "Client " << clientId << " received gossip for the first time: "
         << msg->getContent() << " | local timestamp = " << getTimestamp()
         << " | sender IP = " << getClientIp(msg->getLastHopId())
         << " | sender node = " << msg->getLastHopId();
    logLine(line.str());

    const int previousHop = msg->getLastHopId();
    for (int neighborId : neighborIds) {
        if (neighborId == previousHop) {
            continue;
        }

        RingGossipMessage *copy = msg->dup();
        copy->setLastHopId(clientId);
        sendDirect(copy, getClientModule(neighborId), "directIn");
    }

    delete msg;

    if (static_cast<int>(seenGossipHashes.size()) == numClients) {
        markNodeFinished();
    }
}

void RingClient::markNodeFinished()
{
    if (allGossipReceived) {
        return;
    }

    allGossipReceived = true;
    finishedNodes.insert(clientId);
    logLine("Client " + std::to_string(clientId)
            + " has received gossip from all " + std::to_string(numClients)
            + " clients.");

    if (static_cast<int>(finishedNodes.size()) == numClients) {
        endSimulation();
    }
}

int RingClient::getSuccessorHop(int currentId, const std::map<int, int>& parent) const
{
    int step = currentId;
    while (parent.find(step) != parent.end() && parent.at(step) != clientId) {
        step = parent.at(step);
    }
    return step;
}

int RingClient::getNextHop(int destinationId) const
{
    if (destinationId == clientId) {
        return clientId;
    }

    std::queue<int> pending;
    std::set<int> visited;
    std::map<int, int> parent;

    pending.push(clientId);
    visited.insert(clientId);

    while (!pending.empty()) {
        const int current = pending.front();
        pending.pop();

        auto found = adjacency.find(current);
        if (found == adjacency.end()) {
            continue;
        }

        for (int neighborId : found->second) {
            if (!visited.insert(neighborId).second) {
                continue;
            }

            parent[neighborId] = current;
            if (neighborId == destinationId) {
                return getSuccessorHop(destinationId, parent);
            }

            pending.push(neighborId);
        }
    }

    throw cRuntimeError("No ring path from client %d to client %d",
                        clientId, destinationId);
}

std::string RingClient::getClientIp(int id) const
{
    return "10.0.0." + std::to_string(id + 1);
}

std::string RingClient::getTimestamp() const
{
    std::ostringstream stream;
    stream << simTime();
    return stream.str();
}

std::string RingClient::hashMessage(const std::string& message) const
{
    return std::to_string(std::hash<std::string>{}(message));
}

void RingClient::logLine(const std::string& line)
{
    EV << line << "\n";
    if (outputFile.is_open()) {
        outputFile << line << "\n";
        outputFile.flush();
    }
    if (sharedOutputFile.is_open()) {
        sharedOutputFile << "[Client " << clientId << "] " << line << "\n";
        sharedOutputFile.flush();
    }
}

cModule *RingClient::getClientModule(int id) const
{
    cModule *module = getParentModule()->getSubmodule("client", id);
    if (module == nullptr) {
        throw cRuntimeError("Unable to resolve module for client %d", id);
    }
    return module;
}

void RingClient::handleMessage(cMessage *msg)
{
    if (msg->isSelfMessage()) {
        startLocalTask();
        delete msg;
        return;
    }

    if (auto *taskMessage = dynamic_cast<RingTaskMessage *>(msg)) {
        if (taskMessage->getDestinationClientId() == clientId) {
            processTask(taskMessage);
        } else {
            forwardTask(taskMessage);
        }
        return;
    }

    if (auto *resultMessage = dynamic_cast<RingResultMessage *>(msg)) {
        if (resultMessage->getDestinationClientId() == clientId) {
            handleResult(resultMessage);
        } else {
            forwardResult(resultMessage);
        }
        return;
    }

    if (auto *gossipMessage = dynamic_cast<RingGossipMessage *>(msg)) {
        handleGossip(gossipMessage);
        return;
    }

    delete msg;
}

void RingClient::finish()
{
    if (outputFile.is_open()) {
        outputFile.close();
    }
    if (sharedOutputFile.is_open()) {
        sharedOutputFile.close();
    }
}
