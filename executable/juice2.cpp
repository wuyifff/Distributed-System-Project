#include <fstream>
#include <iostream>
#include <vector>
#include <sstream>
#include <iomanip>
#include <map>

using namespace std;

int main(int args, char** argv) {
    if (args != 3) {
        cout << "Usage: maple <input file> <output file>" << endl;
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
    string line;
    float sum = 0;
    map<string, float> mp;
    while (getline(fin, line)) {
        stringstream ss(line);
        string key, dummy;
        float value;
        ss >> dummy >> key >> value;
        mp[key] = value;
        sum += value;
    }
    for(auto it = mp.begin(); it != mp.end(); it++) {
        string key = it->first;
        for(char& c : key) {
            if(c == '$') {
                c = ' ';
            }
            if(c == '%') {
                c = '/';
            }
            if (c == '@') {
                key = "";
                break;
            }
        }
        key  = "\"" + key + "\"";
        float value = it->second;
        float percent = value / sum;
        fout << key << " " << std::fixed << std::setprecision(2) << percent * 100.0 << "%" << std::endl;
    }
    fout.close();
    return 0;
}