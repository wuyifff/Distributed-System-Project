#include <fstream>
#include <iostream>
#include <vector>
#include <sstream>

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
    stringstream ss(line);
    while (getline(fin, line)) {
        fout << line << endl;
    }
    fout.close();
    return 0;
}