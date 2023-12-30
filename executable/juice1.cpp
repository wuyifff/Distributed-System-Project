#include <fstream>
#include <iostream>
#include <sstream>
#include <map>

using namespace std;

int main(int args, char** argv) {
    if (args != 3) {
        cout << "Usage: juice <input file> <output file>" << endl;
        return 1;
    }
    string inputFilename = argv[1];
    string outputFilename = argv[2];
    ifstream fin(inputFilename);
    ofstream fout(outputFilename);
    if (!fin.is_open()) {
        cout << "Error opening " << inputFilename << endl;
        return 1;
    }
    map<string, int> mp;
    string line;
    while (getline(fin, line)) {
        std::stringstream ss(line);
        std::string key, value;
        ss >> key >> value;
        int v = stoi(value);
        mp[key] += v;
    }
    for(auto i:mp){
        fout<<i.first<<" "<<i.second<<endl;
    }
    fout.close();
}