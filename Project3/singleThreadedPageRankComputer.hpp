#ifndef SRC_SINGLETHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_SINGLETHREADEDPAGERANKCOMPUTER_HPP_

#include <unordered_map>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class SingleThreadedPageRankComputer : public PageRankComputer {
public:
    SingleThreadedPageRankComputer() {};

    std::vector<PageIdAndRank> computeForNetwork(Network const& network,
        double alpha,
        uint32_t iterations,
        double tolerance) const
    {
        std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;
        for (const auto& page : network.getPages()) {
            page.generateId(network.getGenerator());
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

            double dangleSum = 0;
            for (const auto& danglingNode : danglingNodes) {
                dangleSum += previousPageHashMap.at(danglingNode);
            }
            dangleSum = dangleSum * alpha;
            PageRank pageRankWithoutLinks = dangleSum * danglingWeight + (1.0 - alpha) / network.getSize();

            double difference = 0;
            for (auto& pageMapElem : pageHashMap) {
                const PageId& pageId = pageMapElem.first;

                pageMapElem.second = pageRankWithoutLinks;

                if (edges.count(pageId) > 0) {
                    for (const auto& link : edges.at(pageId)) {
                        pageMapElem.second += alpha * previousPageHashMap.at(link)
                            / numLinks.at(link);
                    }
                }
                difference += std::abs(
                    previousPageHashMap.at(pageId) - pageHashMap.at(pageId));
            }

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
        return "SingleThreadedPageRankComputer";
    }
};

#endif /* SRC_SINGLETHREADEDPAGERANKCOMPUTER_HPP_ */
