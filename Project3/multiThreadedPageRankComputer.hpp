#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <thread>

#include <unordered_map>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer {
public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg)
        : numThreads(numThreadsArg) {};

    std::vector<PageIdAndRank> computeForNetwork(Network const& network,
        double alpha,
        uint32_t iterations,
        double tolerance) const
    {
        generateIdentifiers(network);

        std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;
        for (const auto& page : network.getPages()) {
            pageHashMap[page.getId()] = 1.0 / network.getSize();
        }

        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        for (const auto& page : network.getPages()) {
            numLinks[page.getId()] = page.getLinks().size();
        }

        std::vector<PageId> danglingNodes;
        for (const auto& page : network.getPages()) {
            if (page.getLinks().size() == 0) {
                danglingNodes.push_back(page.getId());
            }
        }
        const double danglingWeight = 1.0 / network.getSize();

        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;
        for (const auto& page : network.getPages()) {
            for (const auto& link : page.getLinks()) {
                edges[link].push_back(page.getId());
            }
        }

        for (uint32_t i = 0; i < iterations; ++i) {
            std::unordered_map<PageId, PageRank, PageIdHash>
                previousPageHashMap = pageHashMap;

            double danglingNodesRankSum = getDanglingNodesRankSum(
                danglingNodes,
                previousPageHashMap);

            danglingNodesRankSum *= alpha;
            PageRank
                pageRankWithoutLinks
                = danglingNodesRankSum * danglingWeight
                + (1.0 - alpha) / network.getSize();

            updatePageRank(network,
                pageHashMap,
                previousPageHashMap,
                edges,
                numLinks,
                alpha,
                pageRankWithoutLinks);

            double difference = getDifference(network,
                pageHashMap,
                previousPageHashMap);

            if (difference < tolerance) {
                std::vector<PageIdAndRank> result;
                for (auto iter : pageHashMap) {
                    result.push_back(PageIdAndRank(iter.first, iter.second));
                }

                ASSERT(result.size() == network.getSize(),
                    "Invalid result size=" << result.size()
                                           << ", for network" << network);
                return result;
            }
        }
        ASSERT(false, "Not able to find result in iterations=" << iterations);
    }

    std::string getName() const
    {
        return "MultiThreadedPageRankComputer["
            + std::to_string(this->numThreads) + "]";
    }

private:
    class ThreadsInfo {
    public:
        ThreadsInfo(size_t numberOfNodes, uint32_t numThreads)
        {
            // Don't need more threads than number of nodes.
            numberOfThreadsUsed = std::min(static_cast<uint32_t>(numberOfNodes), numThreads);

            // Divide nodes among threads equally.
            // Threads in first group calculate for one more node than those in second group.
            nodesPerThreadInFirstGroup = numberOfNodes / numThreads + 1;
            nodesPerThreadInSecondGroup = numberOfNodes / numThreads;

            numberOfThreadsInFirstGroup = numberOfNodes % numThreads;
        }

        size_t getThreadFirstIndex(uint32_t threadNumber) const
        {
            if (threadNumber < numberOfThreadsInFirstGroup) {
                return threadNumber * nodesPerThreadInFirstGroup;
            } else {
                return numberOfThreadsInFirstGroup * nodesPerThreadInFirstGroup
                    + (threadNumber - numberOfThreadsInFirstGroup)
                    * nodesPerThreadInSecondGroup;
            }
        }

        size_t getThreadLastIndex(uint32_t threadNumber) const
        {
            if (threadNumber < numberOfThreadsInFirstGroup) {
                return (threadNumber + 1) * nodesPerThreadInFirstGroup - 1;
            } else {
                return numberOfThreadsInFirstGroup * nodesPerThreadInFirstGroup
                    + (threadNumber - numberOfThreadsInFirstGroup + 1)
                    * nodesPerThreadInSecondGroup
                    - 1;
            }
        }

        uint32_t getNumberOfThreadsUsed() const
        {
            return numberOfThreadsUsed;
        }

    private:
        uint32_t numberOfThreadsUsed;
        size_t nodesPerThreadInFirstGroup;
        size_t nodesPerThreadInSecondGroup;
        size_t numberOfThreadsInFirstGroup;
    };

    // Each thread generates id for part of the network.
    static void generateIdentifiersThreadFunction(Network const& network,
        size_t firstPageToUpdate,
        size_t lastPageToUpdate)
    {
        for (size_t index = firstPageToUpdate; index <= lastPageToUpdate;
             ++index) {
            const Page& page = network.getPages().at(index);
            page.generateId(network.getGenerator());
        }
    }

    void generateIdentifiers(Network const& network) const
    {
        ThreadsInfo pagesThreadsInfo(network.getSize(), numThreads);

        std::vector<std::thread> pagesThreads;
        for (uint32_t threadNumber = 0;
             threadNumber < pagesThreadsInfo.getNumberOfThreadsUsed();
             ++threadNumber) {

            size_t firstPageToUpdate = pagesThreadsInfo.getThreadFirstIndex(threadNumber);
            size_t lastPageToUpdate = pagesThreadsInfo.getThreadLastIndex(threadNumber);

            pagesThreads.push_back(std::thread {
                generateIdentifiersThreadFunction,
                std::ref(network),
                firstPageToUpdate,
                lastPageToUpdate });
        }
        for (uint32_t threadNumber = 0;
             threadNumber < pagesThreadsInfo.getNumberOfThreadsUsed();
             ++threadNumber) {
            pagesThreads.at(threadNumber).join();
        }
    }

    // Each thread calculates sum for part of the network.
    static void countDangleSumThreadFunction(
        uint32_t threadNumber,
        const std::vector<PageId>& danglingNodes,
        size_t firstDanglingNodeInSum,
        size_t lastDanglingNodeInSum,
        std::vector<double>& threadDanglingNodesRankSums,
        const std::unordered_map<PageId,
            PageRank,
            PageIdHash>& previousPageHashMap)
    {
        double dangleSum = 0;
        for (size_t index = firstDanglingNodeInSum;
             index <= lastDanglingNodeInSum;
             ++index) {
            dangleSum += previousPageHashMap.at(danglingNodes.at(index));
        }
        threadDanglingNodesRankSums.at(threadNumber) = dangleSum;
    }

    double getDanglingNodesRankSum(
        const std::vector<PageId>& danglingNodes,
        const std::unordered_map<PageId,
            PageRank,
            PageIdHash>& previousPageHashMap) const
    {
        ThreadsInfo danglingNodeThreadsInfo(danglingNodes.size(), numThreads);

        // Sums of ranks of dangling nodes from network.
        // Index is a thread number.
        // Each thread calculates sum for part of the network.
        std::vector<double> threadDanglingNodesRankSums(danglingNodeThreadsInfo.getNumberOfThreadsUsed());

        std::vector<std::thread> danglingNodesThreads;
        for (uint32_t threadNumber = 0; threadNumber
             < danglingNodeThreadsInfo.getNumberOfThreadsUsed();
             ++threadNumber) {

            size_t firstDanglingNodeInSum = danglingNodeThreadsInfo.getThreadFirstIndex(threadNumber);
            size_t lastDanglingNodeInSum = danglingNodeThreadsInfo.getThreadLastIndex(threadNumber);

            danglingNodesThreads.push_back(std::thread {
                countDangleSumThreadFunction,
                threadNumber,
                std::ref(danglingNodes),
                firstDanglingNodeInSum,
                lastDanglingNodeInSum,
                std::ref(threadDanglingNodesRankSums),
                std::ref(previousPageHashMap) });
        }
        for (uint32_t threadNumber = 0; threadNumber
             < danglingNodeThreadsInfo.getNumberOfThreadsUsed();
             ++threadNumber) {
            danglingNodesThreads.at(threadNumber).join();
        }

        double danglingNodesRankSum = 0;
        for (auto sum : threadDanglingNodesRankSums) {
            danglingNodesRankSum += sum;
        }
        return danglingNodesRankSum;
    }

    // Each thread updates pagerank for part of the network.
    static void updatePageRankThreadFunction(
        const Network& network,
        size_t firstPageToUpdate,
        size_t lastPageToUpdate,
        std::unordered_map<PageId, PageRank, PageIdHash>& pageHashMap,
        const std::unordered_map<PageId,
            PageRank,
            PageIdHash>& previousPageHashMap,
        const std::unordered_map<PageId,
            std::vector<PageId>,
            PageIdHash>& edges,
        const std::unordered_map<PageId, uint32_t, PageIdHash>& numLinks,
        double alpha,
        PageRank pageRankWithoutLinks)
    {
        for (size_t index = firstPageToUpdate; index <= lastPageToUpdate;
             ++index) {
            const PageId& pageId = network.getPages().at(index).getId();
            PageRank& pageRank = pageHashMap.at(pageId);
            pageRank = pageRankWithoutLinks;

            if (edges.count(pageId) > 0) {
                for (const auto& link : edges.at(pageId)) {
                    pageRank += alpha * previousPageHashMap.at(link)
                        / numLinks.at(link);
                }
            }
        }
    }

    void updatePageRank(const Network& network,
        std::unordered_map<PageId,
            PageRank,
            PageIdHash>& pageHashMap,
        const std::unordered_map<PageId,
            PageRank,
            PageIdHash>& previousPageHashMap,
        const std::unordered_map<PageId,
            std::vector<PageId>,
            PageIdHash>& edges,
        const std::unordered_map<PageId,
            uint32_t,
            PageIdHash>& numLinks,
        double alpha,
        PageRank pageRankWithoutLinks) const
    {
        ThreadsInfo pagesThreadsInfo(network.getSize(), numThreads);

        std::vector<std::thread> pagesThreads;
        for (uint32_t threadNumber = 0;
             threadNumber < pagesThreadsInfo.getNumberOfThreadsUsed();
             ++threadNumber) {

            size_t firstPageToUpdate = pagesThreadsInfo.getThreadFirstIndex(threadNumber);
            size_t lastPageToUpdate = pagesThreadsInfo.getThreadLastIndex(threadNumber);

            pagesThreads.push_back(std::thread { updatePageRankThreadFunction,
                std::ref(network),
                firstPageToUpdate,
                lastPageToUpdate,
                std::ref(pageHashMap),
                std::ref(previousPageHashMap),
                std::ref(edges),
                std::ref(numLinks),
                alpha,
                pageRankWithoutLinks });
        }
        for (uint32_t threadNumber = 0;
             threadNumber < pagesThreadsInfo.getNumberOfThreadsUsed();
             ++threadNumber) {
            pagesThreads.at(threadNumber).join();
        }
    }

    // Each thread calculates sum for part of the network.
    static void countDifferenceSumThreadFunction(
        uint32_t threadNumber,
        const Network& network,
        size_t firstPageInSum,
        size_t lastPageInSum,
        const std::unordered_map<PageId, PageRank, PageIdHash>& pageHashMap,
        const std::unordered_map<PageId,
            PageRank,
            PageIdHash>& previousPageHashMap,
        std::vector<double>& threadDifferenceRankSums)
    {

        double differenceSum = 0;
        for (size_t index = firstPageInSum; index <= lastPageInSum; ++index) {
            const PageId& pageId = network.getPages().at(index).getId();
            differenceSum += std::abs(
                previousPageHashMap.at(pageId) - pageHashMap.at(pageId));
        }
        threadDifferenceRankSums.at(threadNumber) = differenceSum;
    }

    double getDifference(const Network& network,
        const std::unordered_map<PageId,
            PageRank,
            PageIdHash>& pageHashMap,
        const std::unordered_map<PageId,
            PageRank,
            PageIdHash>& previousPageHashMap) const
    {
        ThreadsInfo pagesThreadsInfo(network.getSize(), numThreads);

        // Sums of differences in pagerank between previousPageHashMap and pageHashMap.
        // Index is a thread number.
        // Each thread calculates sum for part of the network.
        std::vector<double>
            threadDifferenceRankSums(pagesThreadsInfo.getNumberOfThreadsUsed());

        std::vector<std::thread> rankSumDifferenceThreads;
        for (uint32_t threadNumber = 0;
             threadNumber < pagesThreadsInfo.getNumberOfThreadsUsed();
             ++threadNumber) {

            size_t firstPageInSum = pagesThreadsInfo.getThreadFirstIndex(threadNumber);
            size_t lastPageInSum = pagesThreadsInfo.getThreadLastIndex(threadNumber);

            rankSumDifferenceThreads.push_back(std::thread {
                countDifferenceSumThreadFunction,
                threadNumber,
                std::ref(network),
                firstPageInSum,
                lastPageInSum,
                std::ref(pageHashMap),
                std::ref(previousPageHashMap),
                std::ref(threadDifferenceRankSums) });
        }
        for (uint32_t threadNumber = 0;
             threadNumber < pagesThreadsInfo.getNumberOfThreadsUsed();
             ++threadNumber) {
            rankSumDifferenceThreads.at(threadNumber).join();
        }

        double difference = 0;
        for (auto sum : threadDifferenceRankSums) {
            difference += sum;
        }
        return difference;
    }

private:
    uint32_t numThreads;
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
