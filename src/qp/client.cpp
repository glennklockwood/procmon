#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <stack>

#include "QProc.hh"
#include "parseTree.hh"
#include "parser.h"
#include "lexer.h"

#include <boost/filesystem.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/tokenizer.hpp>
#include <boost/token_functions.hpp>

namespace fs = boost::filesystem;
namespace io = boost::iostreams;
namespace algo = boost::algorithm;

extern std::stack<Expression*> parseStack;
QProcConfiguration *configPtr = NULL;
bool defineSymbolsAllowed = true;

void parseExpressions(std::vector<std::string>&, std::vector<Expression*>&, QProcConfiguration&, void (*checkFxn)(const std::string&, const Expression*));
void checkQueryExpression(const std::string&, const Expression*);
void checkDeclarationExpression(const std::string&, const Expression*);

int main(int argc, char **argv) {
    QProcConfiguration config(argc, argv);
    configPtr = &config;
    std::vector<Expression*> declarations;
    std::vector<Expression*> queries;
    std::vector<Expression*> outputColumns;
    std::vector<std::ostream*> outputs;

    if (config.verbose) {
        std::cout << "Output Columns" << std::endl;
        for (unsigned int i = 0; i < config.outputColumns.size(); ++i) std::cout << config.outputColumns[i] << std::endl;
    }


    if (config.queries.size() == 0) {
        std::cout << "At least one query must be specified with \"-q '<query>'\" or in a config file." << std::endl;
        return 1;
    }

    defineSymbolsAllowed = true;
    if (config.declarations.size() > 0) parseExpressions(config.declarations, declarations, config, &checkDeclarationExpression);
    defineSymbolsAllowed = false;
    if (config.queries.size() > 0) parseExpressions(config.queries, queries, config, &checkQueryExpression);
    if (config.outputColumns.size() > 0) parseExpressions(config.outputColumns, outputColumns, config, NULL);

    if (queries.size() > 1 && queries.size() != config.outputFilenames.size()) {
        std::cout << "The number of queries needs to match the number of specified output files" << std::endl;
        return 1;
    }

    intType debug_id = 0;
    bool usingFileOutput = false;
    for (std::vector<std::string>::iterator iter = config.outputFilenames.begin(), end = config.outputFilenames.end(); iter != end; iter++) {
        std::ofstream* out = new std::ofstream((*iter).c_str());
        outputs.push_back(out);
        usingFileOutput = true;
    }

    if (outputs.size() == 0) outputs.push_back(&std::cout);
    for (std::vector<std::ostream*>::iterator iter=outputs.begin(), end=outputs.end(); iter!=end; iter++) {
        std::ostream* out = *iter;
        if (config.header.length() > 0) *out << config.header << std::endl;
    }
}

/* parse vector of expressions from strings into a vector of trees of expressions */
void parseExpressions(std::vector<std::string>& str_expressions, std::vector<Expression*>& out_expressions, QProcConfiguration& config, void (*checkFxn)(const std::string&, const Expression*)) {

	for (std::vector<std::string>::const_iterator iter = str_expressions.begin(), end = str_expressions.end(); iter != end; iter++) {
		Expression* root = NULL;
		YY_BUFFER_STATE scanBuffer = yy_scan_string((*iter).c_str());
		int parseStatus = -1;
		try {
			parseStatus = yyparse();
		} catch (ExpressionException& e) {
			std::cout << "Syntax Error: parsing token \"" << e.getToken() << "\"; " << e.what() << std::endl;
			std::cout << "Exiting." << std::endl;
			exit(1);
		}

		if (parseStatus == 0 && parseStack.size() > 0) {
			if (config.verbose) std::cout << "Parse stack size: " << parseStack.size() << std::endl;
			root = parseStack.top();
			while (parseStack.size() > 0) parseStack.pop();
			if (config.verbose) {
				root->printExpressionTree(0);
				std::cout << std::endl;
			}
			if (root->expressionType == TYPE_EXPRESSIONLIST) {
				ExpressionList* exprList = static_cast<ExpressionList*>(root);
				out_expressions.insert(out_expressions.end(), exprList->expressions.begin(), exprList->expressions.end());
				if (config.verbose) {
					for (std::vector<Expression*>::const_iterator exprIter = exprList->expressions.begin(), end = exprList->expressions.end(); exprIter != end; exprIter++) {
						(*exprIter)->printExpressionTree(0);
						std::cout << std::endl;
					}
				}
				if (checkFxn != NULL) {
					for (std::vector<Expression*>::const_iterator exprIter = exprList->expressions.begin(), end = exprList->expressions.end(); exprIter != end; exprIter++) {
						checkFxn(*iter, *exprIter);
					}
				}
				exprList->expressions.clear(); // safe since expressions is a vector of pointers (no destructor)
				delete root;
			} else {
				if (checkFxn != NULL) checkFxn(*iter, root);
				out_expressions.push_back(root);
			}
		}
		yy_delete_buffer(scanBuffer);
		if (root == NULL) {
			std::cout << "Failed to evaluate expression: '" << (*iter) << "'" << std::endl;
			exit(1);
		}
	}
}

void checkQueryExpression(const std::string& exprString, const Expression* expr) {
    if (expr == NULL || expr->baseType != T_BOOL) {
        std::cout << "Query expression must evaluate to a boolean: " << exprString << std::endl;
        exit(1);
    }
    if (expr->hasType(TYPE_ASSIGN)) {
        std::cout << "Assignments not allowed in query expressions." << std::endl;
        exit(1);
    }
}
void checkDeclarationExpression(const std::string& exprString, const Expression* expr) {
    if (expr == NULL || expr->expressionType != TYPE_ASSIGN) {
        std::cout << "Declaration expressions must be assignments: " << exprString << std::endl;
        exit(1);
    }
}
