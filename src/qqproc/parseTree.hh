#ifndef __QPROC_PARSETREE
#define __QPROC_PARSETREE

#include <string>
#include <stdlib.h>
#include <iostream>
#include <stdexcept>
#include <limits>
#include "QProc.hh"
#include "Function.hh"

#define TYPE_UNKNOWN 0
#define TYPE_NAME 1
#define TYPE_FLOAT 2
#define TYPE_INTEGER 3
#define TYPE_STRING 4
#define TYPE_PLUS 5
#define TYPE_MINUS 6
#define TYPE_TIMES 7
#define TYPE_DIVIDE 8
#define TYPE_BOOL 9
#define TYPE_MODULUS 10
#define TYPE_EXPONENT 11
#define TYPE_EQUAL 12
#define TYPE_NEQUAL 13
#define TYPE_LESS 14
#define TYPE_GREATER 15
#define TYPE_LESSEQUAL 16
#define TYPE_GREATEREQUAL 17
#define TYPE_AND 18
#define TYPE_OR 19
#define TYPE_NOT 20
#define TYPE_GROUP 21
#define TYPE_ASSIGN 22
#define TYPE_VARIABLE 23
#define TYPE_FUNCTION 24
#define TYPE_EXPRESSIONLIST 25

static const char* idMap[] = { "UNKNOWN", "NAME", "FLOAT", "INTEGER", "STRING", "PLUS", "MINUS", "TIMES", "DIVIDE", "BOOL", "MODULUS", "EXPONENT", "EQUAL", "NEQUAL", "LESS", "GREATER", "LESSEQUAL", "GREATEREQUAL", "AND", "OR", "NOT", "GROUP", "ASSIGN", "VARIABLE", "FUNCTION", "EXPRESSIONLIST",};
static const unsigned char fxnType[] = {T_STRING, T_STRING, T_STRING, T_DOUBLE, T_DOUBLE, T_DOUBLE, T_INTEGER, T_DOUBLE, T_BOOL};
static const char* fxnMap[] = {"date", "datetime", "memory", "min", "max", "sum", "count", "mean", "contains"};
static const int fxnCount=9;

class ExpressionException : public std::runtime_error {
private:
	const char* token;
	unsigned char type;
public:
	ExpressionException(const char* t_token, unsigned char t_type, const char* sup) : std::runtime_error(sup), token(t_token), type(t_type) { }
	ExpressionException(const char* t_token, unsigned char t_type, std::string& sup) : std::runtime_error(sup), token(t_token), type(t_type) { }
	const char* getToken() { return token; }
};

class Expression {

public:
	int expressionType;
	unsigned char baseType;
	const char* expressionTypeIdentifier;

	Expression() {
		this->expressionType = TYPE_UNKNOWN;
		this->expressionTypeIdentifier = idMap[TYPE_UNKNOWN];
	}
	virtual ~Expression() { }

	virtual void printExpressionTree(int level) { }
	virtual bool hasType(unsigned char type) const { return type == expressionType; }
	virtual Expression* evaluateExpression(ProcessData *input, Data** data) { return NULL; }
	virtual std::ostream& output(std::ostream& out, ProcessData *input, Data** data) { out << "BASE CLASS UNDEF!"; return out; }

};

class BoolExpression : public Expression {
public:
	bool value;
	BoolExpression(bool t_value): value(t_value) {
		this->expressionType = TYPE_BOOL;
		this->baseType = T_BOOL;
		this->expressionTypeIdentifier = idMap[TYPE_BOOL];
	}
	void printExpressionTree(int level) {
		for (int i = 0; i < level; i++) std::cout << "  ";
		std::cout << "Bln: " << value << std::endl;
	}
	Expression* evaluateExpression(ProcessData *input, Data** data) {
		//std::cout << "DEBUG: Eval BoolExpression " << this->value << std::endl;
		BoolExpression* retValue = new BoolExpression(value);
		return retValue;
	}
	BoolExpression* compare(int type, BoolExpression* b) {
		switch (type) {
			case TYPE_EQUAL:
				this->value = this->value == b->value;
				break;
			case TYPE_NEQUAL:
				this->value = this->value != b->value;
				break;
			case TYPE_LESS:
				this->value = this->value < b->value;
				break;
			case TYPE_GREATER:
				this->value = this->value > b->value;
				break;
			case TYPE_LESSEQUAL:
				this->value = this->value <= b->value;
				break;
			case TYPE_GREATEREQUAL:
				this->value = this->value >= b->value;
				break;
			case TYPE_AND:
				this->value = this->value && b->value;
				break;
			case TYPE_OR:
				this->value = this->value || b->value;
				break;
		}
		delete b;
		return this;
	}
	std::ostream& output(std::ostream& out, ProcessData *input, Data** data) {
		out << value ? "True" : "False";
		return out;
	}
};

class NumericalExpression : public Expression {
public:
	intType i_value;
	double d_value;
	NumericalExpression* sum(NumericalExpression* ptr) {
		bool otherFloat = ptr->expressionType == TYPE_FLOAT;
		bool selfFloat = this->expressionType == TYPE_FLOAT;
		if (!otherFloat && !selfFloat) {
			// integer math & can return self
			this->i_value += ptr->i_value;
			delete ptr;
			return this;
		}
		if (selfFloat) {
			// this is a float, sum and return self
			this->d_value += otherFloat ? ptr->d_value : ptr->i_value;
			delete ptr;
			return this;
		}
		this->d_value = this->i_value + ptr->d_value;
		expressionType = TYPE_FLOAT;
		baseType = T_DOUBLE;
		expressionTypeIdentifier = idMap[TYPE_FLOAT];
		delete ptr;
		return this;
	}
	NumericalExpression* multiply(NumericalExpression* ptr) {
		bool otherFloat = ptr->expressionType == TYPE_FLOAT;
		bool selfFloat = this->expressionType == TYPE_FLOAT;
		if (!otherFloat && !selfFloat) {
			// integer math & can return self
			this->i_value *= ptr->i_value;
			delete ptr;
			return this;
		}
		if (selfFloat) {
			// this is a float, sum and return self
			this->d_value *= otherFloat ? ptr->d_value : ptr->i_value;
			delete ptr;
			return this;
		}
		this->d_value = this->i_value * ptr->d_value;
		expressionType = TYPE_FLOAT;
		baseType = T_DOUBLE;
		expressionTypeIdentifier = idMap[TYPE_FLOAT];
		delete ptr;
		return this;
	}
	NumericalExpression* divide(NumericalExpression* ptr) {
		bool otherFloat = ptr->expressionType == TYPE_FLOAT;
		bool selfFloat = this->expressionType == TYPE_FLOAT;
		if (!otherFloat && !selfFloat) {
			// integer math & can return self
			this->i_value /= ptr->i_value;
			delete ptr;
			return this;
		}
		if (selfFloat) {
			// this is a float, sum and return self
			this->d_value /= otherFloat ? ptr->d_value : ptr->i_value;
			delete ptr;
			return this;
		}
		this->d_value = this->i_value / ptr->d_value;
		expressionType = TYPE_FLOAT;
		baseType = T_DOUBLE;
		expressionTypeIdentifier = idMap[TYPE_FLOAT];
		delete ptr;
		return this;
	}
	NumericalExpression* subtract(NumericalExpression* ptr) {
		bool otherFloat = ptr->expressionType == TYPE_FLOAT;
		bool selfFloat = this->expressionType == TYPE_FLOAT;
		if (!otherFloat && !selfFloat) {
			// integer math & can return self
			this->i_value -= ptr->i_value;
			delete ptr;
			return this;
		}
		if (selfFloat) {
			// this is a float, sum and return self
			this->d_value -= otherFloat ? ptr->d_value : ptr->i_value;
			delete ptr;
			return this;
		}
		this->d_value = this->i_value - ptr->d_value;
		expressionType = TYPE_FLOAT;
		baseType = T_DOUBLE;
		expressionTypeIdentifier = idMap[TYPE_FLOAT];
		delete ptr;
		return this;
	}
	NumericalExpression* modulus(NumericalExpression* ptr) {
		bool otherFloat = ptr->expressionType == TYPE_FLOAT;
		bool selfFloat = this->expressionType == TYPE_FLOAT;
		if (!otherFloat && !selfFloat) {
			// integer math & can return self
			this->i_value %= ptr->i_value;
			delete ptr;
			return this;
		}

		this->i_value = (selfFloat ? (int)this->d_value : this->i_value)  % (otherFloat ? (int)ptr->d_value : ptr->i_value);
		expressionType = TYPE_INTEGER;
		baseType = T_INTEGER;
		expressionTypeIdentifier = idMap[TYPE_INTEGER];
		delete ptr;
		return this;
	}
	NumericalExpression* exponentiate(NumericalExpression* ptr) {
		bool otherFloat = ptr->expressionType == TYPE_FLOAT;
		bool selfFloat = this->expressionType == TYPE_FLOAT;
		if (!otherFloat && !selfFloat) {
			// integer math & can return self
			this->i_value = pow(this->i_value, ptr->i_value);
			delete ptr;
			return this;
		}
		if (selfFloat) {
			// this is a float, sum and return self
			this->d_value = pow(this->d_value, otherFloat ? ptr->d_value : ptr->i_value);
			delete ptr;
			return this;
		}
		this->d_value = pow(this->i_value, ptr->d_value);
		expressionType = TYPE_FLOAT;
		baseType = T_DOUBLE;
		expressionTypeIdentifier = idMap[TYPE_FLOAT];
		delete ptr;
		return this;
	}
	NumericalExpression(intType t_value): i_value(t_value) {
		this->expressionType = TYPE_INTEGER;
		this->baseType = T_INTEGER;
		this->expressionTypeIdentifier = idMap[TYPE_INTEGER];
	}
	NumericalExpression(double t_value): d_value(t_value) {
		this->expressionType = TYPE_FLOAT;
		this->baseType = T_DOUBLE;
		this->expressionTypeIdentifier = idMap[TYPE_FLOAT];
	}
	void printExpressionTree(int level) {
		for (int i = 0; i < level; i++) std::cout << "  ";
		if (this->baseType == T_INTEGER) {
			std::cout << "Int: " << i_value << std::endl;
		} else {
			std::cout << "Dbl: " << d_value << std::endl;
		}
	}
	Expression* evaluateExpression(ProcessData *input, Data** data) {
		//std::cout << "DEBUG: Eval NumericalExpression " << i_value << "," <<  d_value << std::endl;
		if (this->baseType == T_INTEGER) {
			return new NumericalExpression(i_value);
		}
		return new NumericalExpression(d_value);
	}

	BoolExpression* compare(int type, NumericalExpression* b) {
		BoolExpression* retValue = new BoolExpression(false);
		NumericalExpression* a = this; // laziness, don't want to type right now
		bool aFloat = this->baseType == T_DOUBLE;
		bool bFloat = b->baseType == T_DOUBLE;

		switch (type) {
			case TYPE_EQUAL:
				if (aFloat && bFloat) retValue->value = a->d_value == b->d_value;
				else if (aFloat && !bFloat) retValue->value = a->d_value == b->i_value;
				else if (!aFloat && bFloat) retValue->value = a->i_value == b->d_value;
				else retValue->value = a->i_value == b->i_value;
				break;
			case TYPE_NEQUAL:
				if (aFloat && bFloat) retValue->value = a->d_value != b->d_value;
				else if (aFloat && !bFloat) retValue->value = a->d_value != b->i_value;
				else if (!aFloat && bFloat) retValue->value = a->i_value != b->d_value;
				else retValue->value = a->i_value != b->i_value;
				break;
			case TYPE_LESS:
				if (aFloat && bFloat) retValue->value = a->d_value < b->d_value;
				else if (aFloat && !bFloat) retValue->value = a->d_value < b->i_value;
				else if (!aFloat && bFloat) retValue->value = a->i_value < b->d_value;
				else retValue->value = a->i_value < b->i_value;
				break;
			case TYPE_GREATER:
				if (aFloat && bFloat) retValue->value = a->d_value > b->d_value;
				else if (aFloat && !bFloat) retValue->value = a->d_value > b->i_value;
				else if (!aFloat && bFloat) retValue->value = a->i_value > b->d_value;
				else retValue->value = a->i_value > b->i_value;
				break;
			case TYPE_LESSEQUAL:
				if (aFloat && bFloat) retValue->value = a->d_value <= b->d_value;
				else if (aFloat && !bFloat) retValue->value = a->d_value <= b->i_value;
				else if (!aFloat && bFloat) retValue->value = a->i_value <= b->d_value;
				else retValue->value = a->i_value <= b->i_value;
				break;
			case TYPE_GREATEREQUAL:
				if (aFloat && bFloat) retValue->value = a->d_value >= b->d_value;
				else if (aFloat && !bFloat) retValue->value = a->d_value >= b->i_value;
				else if (!aFloat && bFloat) retValue->value = a->i_value >= b->d_value;
				else retValue->value = a->i_value >= b->i_value;
				break;
		}
		delete b;
		return retValue;
	}
	std::ostream& output(std::ostream& out, ProcessData *input, Data** data) {
		if (this->baseType == T_DOUBLE) out << this->d_value;
		else out << this->i_value;
		return out;
	}
};

class StringExpression : public Expression {
public:
	const char* value;
	bool owner;
	StringExpression(const char* t_value, bool t_alloc=false): value(t_value), owner(t_alloc) {
		this->expressionType = TYPE_STRING;
		this->baseType = T_STRING;
		this->expressionTypeIdentifier = idMap[TYPE_STRING];
	}
	~StringExpression() { }
	void printExpressionTree(int level) {
		for (int i = 0; i < level; i++) std::cout << "  ";
		std::cout << "Str: " << value << std::endl;
	}
	Expression* evaluateExpression(ProcessData *input, Data** data) {
		//std::cout << "DEBUG: Eval StringExpression " << this->value << std::endl;
		StringExpression* retValue = new StringExpression(value);
		return retValue;
	}
	BoolExpression* compare(int type, StringExpression* b) {
		BoolExpression* retValue = new BoolExpression(false);
		int cmpVal = strcmp(this->value, b->value);

		switch (type) {
			case TYPE_EQUAL:
				retValue->value = cmpVal == 0;
				break;
			case TYPE_NEQUAL:
				retValue->value = cmpVal != 0;
				break;
			case TYPE_LESS:
				retValue->value = cmpVal < 0;
				break;
			case TYPE_GREATER:
				retValue->value = cmpVal > 0;
				break;
			case TYPE_LESSEQUAL:
				retValue->value = cmpVal <= 0;
				break;
			case TYPE_GREATEREQUAL:
				retValue->value = cmpVal >= 0;
				break;
		}
		delete b;
		return retValue;
	}
	std::ostream& output(std::ostream& out, ProcessData *input, Data** data) {
		out << this->value;
		return out;
	}
};


class NameExpression : public Expression {
public:
	std::string varName;
	unsigned int varIdx;
	unsigned char varType;
	unsigned char varOutputType;
	QProcConfiguration* config;
	NameExpression(char* t_value, QProcConfiguration* t_config, bool allowDef=false): varName(t_value), config(t_config) {
		free(t_value);
		this->expressionType = TYPE_NAME;
		this->expressionTypeIdentifier = idMap[TYPE_NAME];


		/* check to see if this identifier is already in the symbol table
		   if it is, cache the index into the qproc arrays and the type
		   if it isn't, add the new symbol to the table and mark the type
		     as unknown (will be worked out on the first execution of the
			 parse tree)
		*/
		if (config->qVariableMap.count(varName) > 0) {
			varIdx = config->qVariableMap[varName];
			varType = config->qVariables[varIdx].type;
			varOutputType = config->qVariables[varIdx].outputType;
		} else if (allowDef) {
			/* check to see if a fxn name */
			varIdx = config->qVariables.size();
			VarDescriptor newVar;
			snprintf(newVar.name, 30, varName.c_str());
			newVar.type = T_UNDEF;
			newVar.outputType = T_UNDEF;
			config->qVariables.push_back(newVar);
			config->qVariableMap[varName] = varIdx;
			varType = T_UNDEF;
			varOutputType = T_UNDEF;
		} else {
			ExpressionException e(varName.c_str(), TYPE_NAME, "Can't define new symbols here.");
			throw e;
		}
		this->baseType = varType;

	}
	NameExpression(std::string& t_varName, int t_varIdx, int t_varType, QProcConfiguration* t_config): varName(t_varName), varIdx(t_varIdx), varType(t_varType), config(t_config) {
		this->expressionType = TYPE_NAME;
		this->expressionTypeIdentifier = idMap[TYPE_NAME];
	}

	void printExpressionTree(int level) {
		for (int i = 0; i < level; i++) std::cout << "  ";
		std::cout << "Var: " << varName << "," << varIdx << "," << varType << std::endl;
	}
	Expression* evaluateExpression(ProcessData *input, Data** data) {

		if (!(*data)[varIdx].set) { 
            /*
			// need to parse out the resource from the category field (if it is there!)
			char* startPtr = NULL; //strstr(colPtrs[resSourceIdx], config->qVariables[varIdx].name);
			char buffer[128];
			char* endPtr = NULL;
			if (startPtr == NULL) return NULL;
			for ( ; *startPtr != '=' && *startPtr != ',' && *startPtr != 0; startPtr++) { }
			if (*startPtr != '=') return NULL;
			startPtr++; // position on first character of target data (or end position), e.g. h_vmem=10G (move to '1')
			for (endPtr = startPtr; *endPtr != 0 && *endPtr != ',' && *endPtr != ' '; endPtr++) {} // look for end of string

			// determine length of string and stash in buffer
			int bytes = endPtr - startPtr;
			if (bytes > 127) bytes = 127;
			memcpy(buffer, startPtr, bytes);
			buffer[bytes] = 0;
			std::string t_buff(buffer);

			// convert data type
			switch (config->qVariables[varIdx].type) {
				case T_INTEGER:
					(*data)[varIdx].setValue(boost::lexical_cast<intType>(buffer), varOutputType);
					return new NumericalExpression((*data)[varIdx].i_value);
					break;
				case T_DOUBLE:
					(*data)[varIdx].setValue(boost::lexical_cast<double>(buffer), varOutputType);
					return new NumericalExpression((*data)[varIdx].d_value);
					break;
				case T_STRING:
					(*data)[varIdx].setValue(t_buff, varOutputType);
					return new StringExpression(data[varIdx]->s_value);
					break;
				case T_BOOL:
					break;
				case T_MEMORY:
					double prefix = atof(buffer);
					int exp = 0;
					char* ptr = buffer + (bytes - 1);
					if (*ptr == 'p' || *ptr == 'P') exp = 5;
					if (*ptr == 't' || *ptr == 'T') exp = 4;
					if (*ptr == 'g' || *ptr == 'G') exp = 3;
					if (*ptr == 'm' || *ptr == 'M') exp = 2;
					if (*ptr == 'k' || *ptr == 'K') exp = 1;
					(*data)[varIdx].setValue(prefix * pow(1024, exp), varOutputType);
					return new NumericalExpression((*data)[varIdx].d_value);
					break;
			}
		} else if (varIdx < colPtrs.size() || (*data)[varIdx].set) {
			unsigned char type = config->qVariables[varIdx].type;
			if ((*data)[varIdx].set) type = (*data)[varIdx].type;
			switch (type) {
				case T_INTEGER:
					if (!(*data)[varIdx].set && varIdx < colPtrs.size()) {
						(*data)[varIdx].setValue(boost::lexical_cast<intType>(colPtrs[varIdx]), varOutputType);
					}
					return new NumericalExpression((*data)[varIdx].i_value);
					break;
				case T_MEMORY:
				case T_DOUBLE:
					if (!(*data)[varIdx].set && varIdx < colPtrs.size()) {
						(*data)[varIdx].setValue(boost::lexical_cast<double>(colPtrs[varIdx]), varOutputType);
					}
					return new NumericalExpression((*data)[varIdx].d_value);
				case T_STRING:
					if (!(*data)[varIdx].set && varIdx < colPtrs.size()) {
						(*data)[varIdx].setValue(colPtrs[varIdx], varOutputType);
					}
					return new StringExpression((*data)[varIdx].s_value);
					break;
				case T_BOOL:
					return new BoolExpression((*data)[varIdx].b_value);
					break;
				default:
					std::cout << "Error: somehow variable type didn't get set! " << varIdx << ", " << config->qVariables[varIdx].name << std::endl;
			}
        */
		}
		/* this must be a variable we haven't fully typed yet */
		return new NameExpression(varName, varIdx, varType, config);
	}
	std::ostream& output(std::ostream& out, ProcessData *input, Data** data) {
		if ((*data)[varIdx].set) out << (*data)[varIdx];
		else if (varIdx < colPtrs.size()) out << colPtrs[varIdx];
		else out << "ERROR";
		return out;
	}
};

class ExpressionList : public Expression {
	void init() {
		expressionType = TYPE_EXPRESSIONLIST;
		expressionTypeIdentifier = idMap[expressionType];
		baseType = T_UNDEF;
	}
public:
	std::vector<Expression*> expressions;
	ExpressionList() { init(); };
	ExpressionList(Expression* expr1, Expression* expr2) {
		init();
		expressions.push_back(expr1);
		expressions.push_back(expr2);
	}
	~ExpressionList() {
		for (std::vector<Expression*>::const_iterator iter = expressions.begin(), end = expressions.end(); iter != end; iter++) {
			delete *iter;
		}
	}
	void addExpression(Expression* expr1) {
		expressions.push_back(expr1);
	}
	void printExpressionTree(int level) {
		for (int i = 0; i < level; i++) std::cout << "  ";
		std::cout << "ExpList: " << std::endl;
		for (std::vector<Expression*>::const_iterator iter = expressions.begin(), end = expressions.end(); iter != end; iter++) {
			(*iter)->printExpressionTree(level+1);
		}
	}
	virtual bool hasType(unsigned char type) const {
		bool retValue = true;
		for (std::vector<Expression*>::const_iterator iter = expressions.begin(), end = expressions.end(); iter != end; iter++) {
			bool val = (*iter)->hasType(type);
			if (!val) retValue = false;
		}
		return retValue;
	}

	Expression* evaluateExpression(ProcessData *input, Data** data) {
		ExpressionList* ret = new ExpressionList();
		for (std::vector<Expression*>::const_iterator iter = expressions.begin(), end = expressions.end(); iter != end; iter++) {
			ret->expressions.push_back((*iter)->evaluateExpression(input, data));
		}
		return ret;
	}
	std::ostream& output(std::ostream& out, ProcessData *input, Data** data) {
		out << "UNDEF";
		return out;
	}
};

class FxnExpression : public Expression {
public:
	int fxnIdx;
	Function* functionPtr;
	std::string fxnName;
	Expression* expr;
	QProcConfiguration* config;
	FxnExpression(char* t_name, Expression* t_expr, QProcConfiguration* t_config) : fxnName(t_name), expr(t_expr), config(t_config) {
		this->expressionType = TYPE_FUNCTION;
		this->expressionTypeIdentifier = idMap[expressionType];
		this->baseType = T_UNDEF; // this may need to be changed to reflect the target fxn (if type can be determined statically)
		this->fxnIdx = -1;
		this->functionPtr = NULL;

		for (int i = 0; i < fxnCount; i++) {
			if (fxnName == fxnMap[i]) {
				fxnIdx = i;
				this->baseType = fxnType[i];
				
				break;
			}
		}
		switch (fxnIdx) {
			case -1:
				functionPtr = NULL;
				break;
			case 0:
				functionPtr = new DateFunction();
				break;
			case 1:
				functionPtr = new DateTimeFunction();
				break;
			case 2:
				functionPtr = new MemoryFunction();
				break;
			case 3:
				functionPtr = new MinFunction();
				break;
			case 4:
				functionPtr = new MaxFunction();
				break;
			case 5:
				functionPtr = new SumFunction();
				break;
			case 6:
				functionPtr = new CountFunction();
				break;
			case 7:
				functionPtr = new MeanFunction();
				break;
			case 8:
				functionPtr = new ContainsFunction();
				break;
		}
	}
	~FxnExpression() {
		if (functionPtr != NULL) delete functionPtr;
		if (expr != NULL) delete expr;
	}
	void printExpressionTree(int level) {
		for (int i = 0; i < level; i++) std::cout << "  ";
		std::cout << "Fxn: " << fxnName << "," << std::endl;
		expr->printExpressionTree(level+1);
	}
	virtual bool hasType(unsigned char type) const {
		return expressionType == type || expr->hasType(type);
	}
	Expression* evaluateExpression(ProcessData *input, Data** data) {
		Expression* operands = expr->evaluateExpression(input, data);
		Expression* retExpression = NULL;
		ExpressionList* opList = NULL;
		if (operands == NULL) return NULL;
		if (operands->expressionType == TYPE_EXPRESSIONLIST) {
			opList = static_cast<ExpressionList*>(operands);
		}
		/* need to build all this out */

		switch (fxnIdx) {
			case 0: // date
			case 1: // datetime
			{
				DateFunction* fxn = static_cast<DateFunction*>(functionPtr);
				if (!fxn->setFormatStr && opList != NULL && opList->expressions[1]->baseType == T_STRING) {
					StringExpression* fmtArg = static_cast<StringExpression*>(opList->expressions[1]);
					fxn->setFormat(fmtArg->value);
				}
				Expression* argOperand = operands;
				if (opList != NULL) argOperand = opList->expressions[0];
				if (argOperand != NULL && argOperand->baseType == T_INTEGER) {
					NumericalExpression* timeExpr = static_cast<NumericalExpression*>(argOperand);
					retExpression = new StringExpression(fxn->getValue(timeExpr->i_value));
				}
				break;
			}
			case 2: // memory
			{
				MemoryFunction* fxn = static_cast<MemoryFunction*>(functionPtr);
				if (!fxn->setFormatStr && opList != NULL && opList->expressions[1]->baseType == T_STRING) {
					StringExpression* fmtArg = static_cast<StringExpression*>(opList->expressions[1]);
					fxn->setFormat(fmtArg->value);
				}
				Expression* argOperand = operands;
				if (opList != NULL) argOperand = opList->expressions[0];
				if (argOperand != NULL && argOperand->baseType == T_DOUBLE) {
					NumericalExpression* memExpr = static_cast<NumericalExpression*>(argOperand);
					retExpression = new StringExpression(fxn->getValue(memExpr->d_value));
				}
				break;
			}
			case 3: // min
			{
				// need to eventually deal with the output case
				MinFunction* fxn = static_cast<MinFunction*>(functionPtr);
				/* for calculation case, need a list of Numbers */
				if (opList != NULL) {
					bool allNumbers = true;
					int vSize = opList->expressions.size();
					double values[vSize];
					double* vPtr = values;
					int idx = 0;
					for (std::vector<Expression*>::iterator iter = opList->expressions.begin(), end = opList->expressions.end(); iter != end; iter++) {
						NumericalExpression* num = static_cast<NumericalExpression*>(*iter);
						if (*iter == NULL) values[idx++] = std::numeric_limits<double>::max(); // field didn't exist for this record
						else if ((*iter)->baseType == T_INTEGER) values[idx++] = (double) num->i_value;
						else if ((*iter)->baseType == T_DOUBLE) values[idx++] = num->d_value;
						else {
							allNumbers = false;
							break;
						}
					}
					if (allNumbers) {
						double val = fxn->getValue(&vPtr, vSize);
						if (val != std::numeric_limits<double>::max()) {
							retExpression = new NumericalExpression(fxn->getValue(&vPtr, vSize));
						}
					}
				}
				break;
			}
			case 4: // max
			{
				// need to eventually deal with the output case
				MaxFunction* fxn = static_cast<MaxFunction*>(functionPtr);
				/* for calculation case, need a list of Numbers */
				if (opList != NULL) {
					bool allNumbers = true;
					int vSize = opList->expressions.size();
					double values[vSize];
					double* vPtr = values;
					int idx = 0;
					for (std::vector<Expression*>::iterator iter = opList->expressions.begin(), end = opList->expressions.end(); iter != end; iter++) {
						NumericalExpression* num = static_cast<NumericalExpression*>(*iter);
						if (*iter == NULL) values[idx++] = -1 * std::numeric_limits<double>::max(); // field didn't exist for this record
						else if ((*iter)->baseType == T_INTEGER) values[idx++] = (double) num->i_value;
						else if ((*iter)->baseType == T_DOUBLE) values[idx++] = num->d_value;
						else {
							allNumbers = false;
							break;
						}
					}
					if (allNumbers) {
						double val = fxn->getValue(&vPtr, vSize);
						if (val != std::numeric_limits<double>::min()) {
							retExpression = new NumericalExpression(fxn->getValue(&vPtr, vSize));
						}
					}
				}
				break;
			}
			case 5: // sum
			case 6: // count
			case 7: // mean
				break;
			case 8: // contains
			{
				ContainsFunction* fxn = static_cast<ContainsFunction*>(functionPtr);
				/* must be two string arguments - period! */
				if (opList != NULL && opList->expressions.size() == 2 && opList->expressions[0]->baseType == T_STRING && opList->expressions[1]->baseType == T_STRING) {
					StringExpression* tgtString = static_cast<StringExpression*>(opList->expressions[0]);
					if (!fxn->setSearch) {
						StringExpression* searchString = static_cast<StringExpression*>(opList->expressions[1]);
						fxn->setSearchString(searchString->value);
					}
					retExpression = new BoolExpression(fxn->getValue(tgtString->value));
				} else {
					std::cout << "Invalid arguments for contains.  Takes two STRING type arguments." << std::endl;
					exit(1);
				}

				break;
			}
		}
		delete operands;
		return retExpression;
	}
	std::ostream& output(std::ostream& out, ProcessData *input, Data** data) {
		out << "UNDEF";
		return out;
	}

};

class MonExpression : public Expression {
public:
	Expression* expr;
	MonExpression(int expressionType, Expression* t_expr) : expr(t_expr) {
		this->expressionType = expressionType;
		this->expressionTypeIdentifier = idMap[expressionType];
		this->baseType = T_UNDEF;
		if (expressionType == TYPE_NOT) {
			this->baseType = T_BOOL;
		}
		if (expressionType == TYPE_GROUP) {
			this->baseType = expr->baseType;
		}
	}
	~MonExpression() {
		delete expr;
	}
	void printExpressionTree(int level) {
		for (int i = 0; i < level; i++) std::cout << "  ";
		std::cout << expressionTypeIdentifier << std::endl;
		expr->printExpressionTree(level+1);
	}
	virtual bool hasType(unsigned char type) const {
		return expressionType == type || expr->hasType(type);
	}
	Expression* evaluateExpression(ProcessData *input, Data** data) {
		//std::cout << "DEBUG: Eval MonExpression " << this->expressionTypeIdentifier << std::endl;
		Expression* eval = expr->evaluateExpression(input, data);

		if (this->expressionType == TYPE_GROUP) {
			return eval;
		}

		if (this->expressionType == TYPE_NOT && eval->baseType == T_BOOL) {
			BoolExpression* b_eval = static_cast<BoolExpression*>(eval);
			b_eval->value = !(b_eval->value);
			return b_eval;
		}

		std::cout << "No valid expression or expression type for MonExpression->evaluateExpression to evaluate." << std::endl;
		exit(1);
	}
	std::ostream& output(std::ostream& out, ProcessData *input, Data** data) {
		out << "UNDEF";
		return out;
	}
};

class BinExpression : public Expression {
public:
	Expression* left;
	Expression* right;

	BinExpression(int expressionType, Expression* t_left, Expression* t_right): left(t_left), right(t_right) {
		this->expressionType = expressionType;
		this->expressionTypeIdentifier = idMap[expressionType];
		this->baseType = T_UNDEF;

		if (expressionType == TYPE_AND || expressionType == TYPE_OR || expressionType == TYPE_EQUAL || expressionType == TYPE_NEQUAL || expressionType == TYPE_LESS || expressionType == TYPE_GREATER || expressionType == TYPE_LESSEQUAL || expressionType == TYPE_GREATEREQUAL) {
			this->baseType = T_BOOL;
		}
	}

	~BinExpression() {
		delete left;
		delete right;
	}
	void printExpressionTree(int level) {
		for (int i = 0; i < level; i++) std::cout << "  ";
		std::cout << expressionTypeIdentifier << std::endl;
		left->printExpressionTree(level+1);
		right->printExpressionTree(level+1);
	}
	virtual bool hasType(unsigned char type) const {
		return expressionType == type || left->hasType(type) || right->hasType(type);
	}
	Expression* evaluateExpression(ProcessData *input, Data** data) {
		//std::cout << "DEBUG: Eval BinExpression " << this->expressionTypeIdentifier << std::endl;
		Expression* eval_left = left->evaluateExpression(input, data);
		Expression* eval_right = right->evaluateExpression(input, data);

		if (eval_left == NULL || eval_right == NULL) return NULL;

		switch (expressionType) {
			case TYPE_PLUS:
			case TYPE_MINUS:
			case TYPE_TIMES:
			case TYPE_DIVIDE:
			case TYPE_MODULUS:
			case TYPE_EXPONENT:
				return this->mathOperation(eval_left, eval_right);
				break;
			case TYPE_EQUAL:
			case TYPE_NEQUAL:
			case TYPE_LESS:
			case TYPE_GREATER:
			case TYPE_LESSEQUAL:
			case TYPE_GREATEREQUAL:
				return this->compareOperation(eval_left, eval_right);
				break;
			case TYPE_AND:
			case TYPE_OR:
				return this->booleanOperation(eval_left, eval_right);
				break;
			case TYPE_ASSIGN:
				return this->assignOperation(eval_left, eval_right, data);
				break;
		}
		return NULL;
	}

	NumericalExpression* mathOperation(Expression* eval_left, Expression* eval_right) {
		if (eval_left == NULL || eval_right == NULL) return NULL;

		if ((eval_left->baseType == T_INTEGER || eval_left->baseType == T_DOUBLE) &&
			(eval_right->baseType == T_INTEGER || eval_right->baseType == T_DOUBLE)) {

			NumericalExpression* neval_left = static_cast<NumericalExpression*>(eval_left);
			NumericalExpression* neval_right = static_cast<NumericalExpression*>(eval_right);

			if (expressionType == TYPE_PLUS) return neval_left->sum(neval_right);
			if (expressionType == TYPE_MINUS) return neval_left->subtract(neval_right);
			if (expressionType == TYPE_TIMES) return neval_left->multiply(neval_right);
			if (expressionType == TYPE_DIVIDE) return neval_left->divide(neval_right);
			if (expressionType == TYPE_MODULUS) return neval_left->modulus(neval_right);
			if (expressionType == TYPE_EXPONENT) return neval_left->exponentiate(neval_right);
		}
		return NULL;
	}

	BoolExpression* compareOperation(Expression* eval_left, Expression* eval_right) {
		/* need to check types of each expression;
		   can compare float/int to float/int
		   can compare string to string
		   can compare bool to bool
		   All other combinations fail
		*/
		if (eval_left == NULL || eval_right == NULL) return NULL;

		if ((eval_left->baseType == T_INTEGER || eval_left->baseType == T_DOUBLE) &&
			(eval_right->baseType == T_INTEGER || eval_right->baseType == T_DOUBLE)) {

			NumericalExpression* neval_left = static_cast<NumericalExpression*>(eval_left);
			NumericalExpression* neval_right = static_cast<NumericalExpression*>(eval_right);

			BoolExpression* ret = neval_left->compare(expressionType, neval_right);
			delete neval_left;
			return ret;
		}
		if (eval_left->baseType == T_STRING && eval_right->baseType == T_STRING) {
			StringExpression* s_eval_left = static_cast<StringExpression*>(eval_left);
			StringExpression* s_eval_right = static_cast<StringExpression*>(eval_right);

			BoolExpression* ret = s_eval_left->compare(expressionType, s_eval_right);
			delete s_eval_left;
			return ret;
		}
		if (eval_left->baseType == T_BOOL && eval_right->baseType == T_BOOL) {
			BoolExpression* b_eval_left = static_cast<BoolExpression*>(eval_left);
			BoolExpression* b_eval_right = static_cast<BoolExpression*>(eval_right);

			return b_eval_left->compare(expressionType, b_eval_right);
		}
		return NULL;
	}

	BoolExpression* booleanOperation(Expression* eval_left, Expression* eval_right) {
		if (eval_left->baseType == T_BOOL && eval_right->baseType == T_BOOL) {
			BoolExpression* b_eval_left = static_cast<BoolExpression*>(eval_left);
			BoolExpression* b_eval_right = static_cast<BoolExpression*>(eval_right);

			return b_eval_left->compare(expressionType, b_eval_right);
		}
		return NULL;
	}

	Expression* assignOperation(Expression* eval_left, Expression* eval_right, Data** data) {
		/* eval_left has to be a NameExpression
		   eval_right has to be a primitive of some type: String, Int, Float, or Bool
		   need to store the right value into data[eval_left->varIdx], then
		   return right value
		*/
		if (eval_left->expressionType != TYPE_NAME) return NULL;
		NameExpression* varExpr = static_cast<NameExpression*>(eval_left);
		//if (data[varExpr->varIdx] == NULL) data[varExpr->varIdx] = new Data();

		if (eval_right->baseType == T_INTEGER || eval_right->baseType == T_DOUBLE) {
			NumericalExpression* intExpr = static_cast<NumericalExpression*>(eval_right);
			if (intExpr->baseType == T_INTEGER) {
				(*data)[varExpr->varIdx].setValue(intExpr->i_value);
			} else {
				(*data)[varExpr->varIdx].setValue(intExpr->d_value);
			}
			delete eval_left;
			return eval_right;
		}
		if (eval_right->baseType == T_STRING) {
			StringExpression* strExpr = static_cast<StringExpression*>(eval_right);
			(*data)[varExpr->varIdx].setValue(strExpr->value);
			delete eval_left;
			return eval_right;
		}
		if (eval_right->baseType == T_BOOL) {
			BoolExpression* bExpr = static_cast<BoolExpression*>(eval_right);
			(*data)[varExpr->varIdx].setValue(bExpr->value);
			delete eval_left;
			return eval_right;
		}

		return NULL;
	}
	std::ostream& output(std::ostream& out, ProcessData *input, Data** data) {
		out << "UNDEF";
		return out;
	}
};

	
#endif /* __QACCT_PARSETREE */
