#include "config.h"
#include "ProcData.hh"
#include "ProcIO.hh"
#include <signal.h>
#include <string.h>
#include <iostream>
#include "ProcReducerData.hh"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/program_options.hpp>
#define CHECKH5_VERSION 2.5
namespace po = boost::program_options;

/* ProcReducer
    * Takes in procmon data from all sources and only writes the minimal record
    * Writes an HDF5 file for all procdata foreach day
*/

void version() {
    cout << "CheckH5 " << CHECKH5_VERSION;
    cout << endl;
    exit(0);
}

class CheckH5Config {
public:
    CheckH5Config(int argc, char **argv) {
        /* Parse command line arguments */
        po::options_description basic("Basic Options");
        basic.add_options()
            ("version", "Print version information")
            ("help,h", "Print help message")
            ("verbose,v", "Print extra (debugging) information")
        ;
        po::options_description config("Configuration Options");
        config.add_options()
            ("input,i",po::value<std::string>(&(this->input_filename)), "input filename (required)")
        ;

        po::options_description options;
        options.add(basic).add(config);
        po::variables_map vm;
        try {
            po::store(po::command_line_parser(argc, argv).options(options).run(), vm);
            po::notify(vm);
            if (vm.count("help")) {
                std::cout << options << std::endl;
                exit(0);
            }
            if (vm.count("version")) {
                version();
                exit(0);
            }
            if (vm.count("debug")) {
                debug = true;
            }
        } catch (std::exception &e) {
            std::cout << e.what() << std::endl;
            std::cout << options << std::endl;
            exit(1);
        }
    }
    bool debug;

    string input_filename;
};

void readH5File(const string& filename) {
    ProcHDF5IO *inputFile = new ProcHDF5IO(filename, FILE_MODE_READ);
    vector<string> hosts;

    inputFile->get_hosts(hosts);
    for (auto ptr = hosts.begin(), end = hosts.end(); ptr != end; ++ptr) {
        hostname = *ptr;
        identifier = "any";
        subidentifier = "any";
        intputFile->set_context(hostname, identifier, subidentifier);


    }
}


int main(int argc, char **argv) {
    CheckH5Config config(argc, argv);


    string hostname, identifier, subidentifier;

    int saveCnt = 0;
    int nRecords = 0;

    char buffer[1024];
    vector<string> hosts;
    inputFile->get_hosts(hosts);
    cout << "\"hosts\":{" << endl;
    for (auto ptr = hosts.begin(), end = hosts.end(); ptr != end; ++ptr) {
        hostname = *ptr;
        identifier = "any";
        subidentifier = "any";
        inputFile->set_context(hostname, identifier, subidentifier);
        int n_procdata = inputFile->get_nprocdata();
        int n_procstat = inputFile->get_nprocstat();
        int n_procfd = inputFile->get_nprocfd();
        int n_procobs = inputFile->get_nprocobs();

        cout << "\"" << hostname << "\": {" << endl;
        cout << "  \"nprocdata\":" << n_procdata << "," << endl;
        cout << "  \"nprocstat\":" << n_procstat << "," << endl;
        cout << "  \"nprocfd\":" << n_procfd << "," << endl;
        cout << "  \"nprocobs\":" << n_procobs << endl;
        cout << "}";
        if (ptr + 1 != hosts.end()) cout << ",";
        cout << endl;
    }
    cout << "}" << endl;
    cout << "}" << endl;

    delete inputFile;

    return 0;
}
