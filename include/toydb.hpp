#ifndef TOYDB_HPP
#define TOYDB_HPP

#include <string>
#include <vector>
#include <optional>

class ToyDB {
public:
    ToyDB();
    ~ToyDB();

    void insert(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
    bool remove(const std::string& key);

private:
    struct Record {
        std::string key;
        std::string value;
    };

    std::vector<Record> records;
};

#endif // TOYDB_HPP