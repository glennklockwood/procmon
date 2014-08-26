#ifndef __FUNCTION_H
#define __FUNCION_H

#include <limits>
#include "QProc.hh"
#include <iostream>
#include <string.h>

class Function {
public:
	bool groupFxn;
	bool outputFxn;
	unsigned char baseType;
	bool isGroupingFunction() { return groupFxn; }
	bool isOutputFunction() { return outputFxn; }
};

class MinFunction : public Function {
public:
	double value;
	MinFunction() {
		value = std::numeric_limits<double>::max();
		groupFxn = true;
		outputFxn = true;
		baseType = T_DOUBLE;
	}
	inline void applyFxn(double t_value) {
		if (t_value < value) value = t_value;
	}
	inline void applyFxn(int t_value) {
		if (t_value < value) value = t_value;
	}
	inline double getValue(double** val, int n) {
		double ref = std::numeric_limits<double>::max();
		for (int i = 0; i < n; i++) {
			if ((*val)[i] < ref) {
				ref = (*val)[i];
			}
		}
		return ref;
	}
	inline double getValue() {
		return value;
	}
};

class MaxFunction : public Function {
private:
	double value;
public:
	MaxFunction() {
		value = -1 * std::numeric_limits<double>::max();
		groupFxn = true;
		outputFxn = true;
		baseType = T_DOUBLE;
	}
	inline void applyFxn(double t_value) {
		if (t_value > value) value = t_value;
	}
	inline void applyFxn(int t_value) {
		if (t_value > value) value = t_value;
	}
	inline double getValue(double** val, int n) {
		double ref = -1 * std::numeric_limits<double>::min();
		for (int i = 0; i < n; i++) {
			if ((*val)[i] > ref) {
				ref = (*val)[i];
			}
		}
		return ref;
	}
	inline double getValue() {
		return value;
	}
};

class CountFunction : public Function {
private:
	int value;
public:
	CountFunction() {
		value = 0;
		groupFxn = true;
		outputFxn = true;
		baseType = T_INTEGER;
	}
	inline void applyFxn() {
		value++;
	}
};

class MeanFunction : public Function {
private:
	double sum;
	int count;
public:
	MeanFunction() {
		sum = 0;
		count = 0;
		groupFxn = true;
		outputFxn = true;
		baseType = T_DOUBLE;
	}
	inline void applyFxn(double value) {
		sum += value;
		count++;
	}
	inline void applyFxn(int value) {
		sum += value;
		count++;
	}
	void output(std::ostream& out) {
		out << sum / count;
	}
};

class SumFunction : public Function {
private:
	double sum;
public:
	SumFunction() {
		sum = 0;
		groupFxn = true;
		outputFxn = true;
		baseType = T_DOUBLE;
	}
	inline void applyFxn(double value) {
		sum += value;
	}
	void output(std::ostream& out) {
		out << sum;
	}
};

class ContainsFunction : public Function {
private:
	std::string search;
public:
	bool setSearch;
	ContainsFunction() {
		groupFxn = false;
		outputFxn = false;
		baseType = T_BOOL;
		setSearch = false;
	}
	void setSearchString(std::string& t_search) { search = t_search; setSearch = true;}
	void setSearchString(const char* t_search) { search = t_search; setSearch = true;}
	inline bool getValue(const char* target) {
		return strstr(target, search.c_str()) != NULL;
	}
};

class DateFunction : public Function {
public:
	std::string dateStr;
	bool setFormatStr;
	std::string formatStr;
	struct tm timeStruct;
	static const int maxLen = 64;
	char buffer[maxLen];
	
	DateFunction() {
		init();
		formatStr = "%Y-%m-%d";
	}

	void init() {
		groupFxn = false;
		outputFxn = true;
		setFormatStr = false;
		baseType = T_STRING;
		memset(&timeStruct, 0, sizeof(struct tm));
		buffer[0] = 0;
	}
	bool setFormat(const std::string& t_formatStr) {
		formatStr = t_formatStr;
		setFormatStr = true;
		return true;
	}
		
	const char* getValue(time_t timestamp) {
		localtime_r(&timestamp, &timeStruct);
		strftime(buffer, maxLen, formatStr.c_str(), &timeStruct);
		return buffer;
	}
};


class DateTimeFunction : public DateFunction {
public:
	DateTimeFunction() {
		init();
		formatStr = "%Y-%m-%d %H:%M:%S";
	}
};

class MemoryFunction : public Function {
public:
	static const int maxBufferLen = 1024;
	char buffer[maxBufferLen];
	int exp;
	const char* allFmt;
	bool setFormatStr;

	MemoryFunction() {
		allFmt = "BKMGTPE";
		groupFxn = false;
		outputFxn = true;
		buffer[0] = 0;
		exp = -1; // auto
		setFormatStr = false;
	}
	bool setFormat(const std::string& t_format) {
		char type = toupper(t_format[0]);
		int maxFmt = strlen(allFmt);
		for (exp = 0; exp < maxFmt; exp++) {
			if (type == allFmt[exp]) break;
		}
		if (exp == maxFmt) {
			exp = -1;
			return false;
		}
		setFormatStr = true;
		return true;
	}
	const char* getValue(double val) {
		bool autoFormat = false;
		if (exp < 0) {
			autoFormat = true;
			exp = 0;
			bool neg = val < 0;
			if (neg) val *= -1;
			while (val > 1000) {
				val /= 1024;
				exp++;
			}
			if (neg) val *= -1;
		} else {
			val /= pow(1024, exp);
		}
		snprintf(buffer, maxBufferLen, "%3.2f%c", val, allFmt[exp]);
		if (autoFormat) exp = -1;
		return buffer;
	}
};

class GetentUserFunction : public Function {
};


#endif
