#include <fstream>
#include <iostream>
#include <vector>
#include <sstream>

using namespace std;

int main(int args, char** argv) {
    if (args != 4) {
        cout << "Usage: maple <input file> <output file> <extra>" << endl;
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
    vector< vector<string> > data;
    string line;
    stringstream ss(line);
    string value;
    vector<string> row;
    int index = 9;
    while (getline(fin, line)) {
        stringstream ss(line);
        string value;
        vector<string> row;
        while (getline(ss, value, ',')) {
            row.push_back(value);
        }
        if(row[index+1] == argv[3]) {
            cout << row[index+1] << endl;
            string s = row[index];
            // cout << s << endl;
            for(int i = 0; i < s.size(); i++) {
                if(s[i] == ' ') {
                    s[i] = '$';
                }
                if(s[i] == '/') {
                    s[i] = '%';
                }
            }
            if(s == "") {
                s = "@";
            }
            fout << s << ' ' << 1 << endl;
        }
    }
    fout.close();
    return 0;
}