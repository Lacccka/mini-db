#include "kv/kvstore.h"
#include <iostream>
#include <sstream>

int main() {
    try {
        Config cfg;
        cfg.data_dir = L"./data";
        cfg.segment_max_bytes = 8ull * 1024 * 1024;
        cfg.fsync_each_write = true;

        KVStore db(cfg);
        std::cout << "MiniDB (SET key value | GET key | DEL key | COMPACT | EXIT)\n";

        std::string line;
        while (true) {
            std::cout << "> " << std::flush;
            if (!std::getline(std::cin, line)) break;
            std::istringstream iss(line);
            std::string cmd; iss >> cmd;
            if (cmd=="SET") {
                std::string key; iss >> key;
                std::string value; std::getline(iss, value);
                if (!value.empty() && value[0]==' ') value.erase(0,1);
                if (key.empty()){ std::cout<<"usage: SET <key> <value>\n"; continue; }
                db.set(key, value);
                std::cout << "OK\n";
            } else if (cmd=="GET") {
                std::string key; iss >> key;
                if (key.empty()){ std::cout<<"usage: GET <key>\n"; continue; }
                auto v = db.get(key);
                std::cout << (v ? *v : "(nil)") << "\n";
            } else if (cmd=="DEL") {
                std::string key; iss >> key;
                if (key.empty()){ std::cout<<"usage: DEL <key>\n"; continue; }
                std::cout << (db.del(key) ? "OK" : "NOT FOUND") << "\n";
            } else if (cmd=="COMPACT") {
                db.compact();
                std::cout << "COMPACTED\n";
            } else if (cmd=="EXIT") {
                break;
            } else {
                std::cout << "Unknown command\n";
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
