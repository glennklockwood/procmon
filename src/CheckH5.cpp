/*******************************************************************************
procmon, Copyright (c) 2014, The Regents of the University of California,
through Lawrence Berkeley National Laboratory (subject to receipt of any
required approvals from the U.S. Dept. of Energy).  All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Technology Transfer Department at  TTD@lbl.gov.

The LICENSE file in the root directory of the source code archive describes the
licensing and distribution rights and restrictions on this software.

Author:   Douglas Jacobsen <dmj@nersc.gov>
*******************************************************************************/

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

int main(int argc, char **argv) {
    CheckH5Config config(argc, argv);

    ProcHDF5IO* inputFile = new ProcHDF5IO(config.input_filename, FILE_MODE_READ);

    unsigned long long_data;
    char *str_ptr;
    const char *string_identifiers[] = {
        "writer", "writer_version", "writer_host", "source",
    };
    const char *int_identifiers[] = {
        "recording_start","recording_stop","n_writes",
    };
    cout << "{" << endl;
    for (int i = 0; i < 4; i++) {
        inputFile->metadata_get_string(string_identifiers[i], &str_ptr);
        cout << "\"" << string_identifiers[i] << "\":\"" << str_ptr << "\"," << endl;
        free(str_ptr);
    }
    for (int i = 0; i < 3; i++) {
        inputFile->metadata_get_uint(int_identifiers[i], &long_data);
        cout << "\"" << int_identifiers[i] << "\":" << long_data << "," << endl;
    }

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
