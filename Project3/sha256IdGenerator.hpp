#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include <array>
#include <memory>
#include <unistd.h>

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"

class Sha256IdGenerator : public IdGenerator {
public:
    PageId generateId(std::string const& content) const override
    {
        char filename[] = "tempfile-XXXXXX";
        int fd = mkstemp(filename);
        ASSERT(fd != -1, "mkstemp error");

        FILE* fptr = fdopen(fd, "w");
        ASSERT(fptr != nullptr, "fdopen error");
        ASSERT(fprintf(fptr, "%s", content.c_str()) >= 0, "fprintf error");
        ASSERT(fclose(fptr) == 0, "fclose error");

        std::string command = "sha256sum ";
        command.append(filename);
        std::string commandResult = execCmd(command);

        ASSERT(std::remove(filename) == 0, "remove file error");

        size_t spacePos = commandResult.find(' ');
        std::string id = commandResult.substr(0, spacePos);

        return PageId(id);
    }

private:
    static std::string execCmd(const std::string& cmd)
    {
        std::array<char, 128> buffer {};
        std::string result;
        std::unique_ptr<FILE, decltype(&pclose)>
            pipe(popen(cmd.c_str(), "r"), pclose);
        ASSERT(pipe, "popen error");
        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            result += buffer.data();
        }
        return result;
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */
