#include "Client.h"
#include <time>
#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>
#include <sstream>

Define_Module(ChordClient);

std::set<int> ChordClient::finishedNodes;

void ChordClient::initialize()
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
    buildFingerTable();
    initializeTaskData();

    logLine("Client " + std::to_string(clientId) + " initialized with "
            + std::to_string(neighborIds.size()) + " CHORD neighbors.");

    cMessage *startMessage = new cMessage("startLocalTask");
    scheduleAt(simTime() + SimTime(0.1 + (0.01 * clientId)), startMessage);
}

void ChordClient::loadTopology()
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
        if (nodeId != clientId) {
            continue;
        }

        int neighbor = -1;
        while (parser >> neighbor) {
            if (neighbor >= 0 && neighbor < numClients && neighbor != clientId) {
                if (std::find(neighborIds.begin(), neighborIds.end(), neighbor) == neighborIds.end()) {
                    neighborIds.push_back(neighbor);
                }
            }
        }
        break;
    }

    if (neighborIds.empty()) {
        throw cRuntimeError("Client %d has no neighbors configured in '%s'",
                            clientId, topologyFile.c_str());
    }
}

void ChordClient::buildFingerTable()
{
    fingerTable.clear();
    const int entries = std::max(1, static_cast<int>(std::ceil(std::log2(static_cast<double>(numClients)))));
    for (int index = 0; index < entries; ++index) {
        fingerTable.push_back((clientId + (1 << index)) % numClients);
    }

    std::ostringstream line;
    line << "Client " << clientId << " finger table:";
    for (int finger : fingerTable) {
        line << ' ' << finger;
    }
    logLine(line.str());
}

void ChordClient::initializeTaskData()
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

int ChordClient::determineSubtaskCount() const
{
    if (arraySize < 2) {
        return 1;
    }

    int maxSubtasks = std::max(1, arraySize / 2);
    return std::max(1, std::min(requestedSubtasks, maxSubtasks));
}

void ChordClient::startLocalTask()
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

        ChordTaskMessage *taskMessage = new ChordTaskMessage("chordTask");
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
                + " via CHORD neighbor " + std::to_string(nextHop) + ".");

        sendDirect(taskMessage, getClientModule(nextHop), "directIn");
    }
}

void ChordClient::processTask(ChordTaskMessage *msg)
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

void ChordClient::sendResult(int taskOwnerId, int subtaskId, int result)
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

    ChordResultMessage *resultMessage = new ChordResultMessage("chordResult");
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

void ChordClient::forwardTask(ChordTaskMessage *msg)
{
    const int nextHop = getNextHop(msg->getDestinationClientId());
    msg->setHopCount(msg->getHopCount() + 1);
    msg->setLastHopId(clientId);
    sendDirect(msg, getClientModule(nextHop), "directIn");
}

void ChordClient::forwardResult(ChordResultMessage *msg)
{
    const int nextHop = getNextHop(msg->getDestinationClientId());
    msg->setHopCount(msg->getHopCount() + 1);
    msg->setLastHopId(clientId);
    sendDirect(msg, getClientModule(nextHop), "directIn");
}

void ChordClient::handleResult(ChordResultMessage *msg)
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

void ChordClient::broadcastGossip()
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
        ChordGossipMessage *gossipMessage = new ChordGossipMessage("chordGossip");
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

void ChordClient::handleGossip(ChordGossipMessage *msg)
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

        ChordGossipMessage *copy = msg->dup();
        copy->setLastHopId(clientId);
        sendDirect(copy, getClientModule(neighborId), "directIn");
    }

    delete msg;

    if (static_cast<int>(seenGossipHashes.size()) == numClients) {
        markNodeFinished();
    }
}

void ChordClient::markNodeFinished()
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

int ChordClient::clockwiseDistance(int fromId, int toId) const
{
    return (toId - fromId + numClients) % numClients;
}

int ChordClient::getSuccessor() const
{
    int bestHop = neighborIds.front();
    int bestDistance = numClients + 1;

    for (int neighborId : neighborIds) {
        const int distance = clockwiseDistance(clientId, neighborId);
        if (distance > 0 && distance < bestDistance) {
            bestDistance = distance;
            bestHop = neighborId;
        }
    }

    return bestHop;
}

int ChordClient::getNextHop(int destinationId) const
{
    if (destinationId == clientId) {
        return clientId;
    }

    if (std::find(neighborIds.begin(), neighborIds.end(), destinationId) != neighborIds.end()) {
        return destinationId;
    }

    const int distanceToDestination = clockwiseDistance(clientId, destinationId);
    int bestHop = getSuccessor();
    int bestProgress = 0;

    for (int neighborId : neighborIds) {
        const int progress = clockwiseDistance(clientId, neighborId);
        if (progress > 0 && progress < distanceToDestination && progress > bestProgress) {
            bestProgress = progress;
            bestHop = neighborId;
        }
    }

    return bestHop;
}

std::string ChordClient::getClientIp(int id) const
{
    return "10.0.0." + std::to_string(id + 1);
}

std::string ChordClient::getTimestamp() const
{
    std::ostringstream stream;
    stream << simTime();
    return stream.str();
}

std::string ChordClient::hashMessage(const std::string& message) const
{
    return std::to_string(std::hash<std::string>{}(message));
}

void ChordClient::logLine(const std::string& line)
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

cModule *ChordClient::getClientModule(int id) const
{
    cModule *module = getParentModule()->getSubmodule("client", id);
    if (module == nullptr) {
        throw cRuntimeError("Unable to resolve module for client %d", id);
    }
    return module;
}

void ChordClient::handleMessage(cMessage *msg)
{
    if (msg->isSelfMessage()) {
        startLocalTask();
        delete msg;
        return;
    }

    if (auto *taskMessage = dynamic_cast<ChordTaskMessage *>(msg)) {
        if (taskMessage->getDestinationClientId() == clientId) {
            processTask(taskMessage);
        } else {
            forwardTask(taskMessage);
        }
        return;
    }

    if (auto *resultMessage = dynamic_cast<ChordResultMessage *>(msg)) {
        if (resultMessage->getDestinationClientId() == clientId) {
            handleResult(resultMessage);
        } else {
            forwardResult(resultMessage);
        }
        return;
    }

    if (auto *gossipMessage = dynamic_cast<ChordGossipMessage *>(msg)) {
        handleGossip(gossipMessage);
        return;
    }

    delete msg;
}

void ChordClient::finish()
{
    if (outputFile.is_open()) {
        outputFile.close();
    }
    if (sharedOutputFile.is_open()) {
        sharedOutputFile.close();
    }
}
