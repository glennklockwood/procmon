"""procmon/Scriptable contains classes describing different scriptable
   executables and provides methods to attempt to identify the script name"""

import re

def Scriptable(execName, commandArgs):
    if execName == "java":
        return JavaScriptable(execName, commandArgs)
    if execName.startswith("python"):
        return PythonScriptable(execName, commandArgs)
    if execName.startswith("perl"):
        return PerlScriptable(execName, commandArgs)
    if execName.startswith("ruby"):
        return RubyScriptable(execName, commandArgs)
    if re.match("^(ba)?sh.*", execName) is not None:
        return BashScriptable(execName, commandArgs)
    if re.match("^(t)?csh.*", execName) is not None:
        return CshScriptable(execName, commandArgs)
    return None

def JavaScriptable(execName, commandArgs):
    idx = 0
    while idx < len(commandArgs):
        if commandArgs[idx] == "-jar":
            idx += 1
            if idx < len(commandArgs):
                # return the name of the executable jar file
               return commandArgs[idx]; 
        if commandArgs[idx] == "-cp":
            # skip next arg
            idx += 1
        elif commandArgs[idx].startswith("-"):
            # skip this arg
            pass
        else:
            # class name
            return commandArgs[idx]
        idx += 1
    return None

def PythonScriptable(execName, commandArgs):
    idx = 0
    while idx < len(commandArgs):
        if commandArgs[idx] in ("-m", "-Q", "-W",):
            #skip next arg
            idx += 1
        elif commandArgs[idx] == "-c":
            return "COMMAND"
        elif commandArgs[idx].startswith("-"):
            #skip this arg
            pass
        else:
            return commandArgs[idx]
        idx += 1
    return None

def PerlScriptable(execName, commandArgs):
    idx = 0
    while idx < len(commandArgs):
        if commandArgs[idx].startswith("-e") or commandArgs[idx].startswith("-E"):
            return "COMMAND";
        elif commandArgs[idx].startswith("-"):
            pass
        else:
            return commandArgs[idx]
        idx += 1
    return None

def RubyScriptable(execName, commandArgs):
    idx = 0
    while idx < len(commandArgs):
        if commandArgs[idx] in ("-C","-F","-I","-K","-r"):
            idx += 1
        elif commandArgs[idx] == "-e":
            return "COMMAND"
        elif commandArgs[idx].startswith("-"):
            pass
        else:
            return commandArgs[idx]
        idx += 1
    return None

def BashScriptable(execName, commandArgs):
    idx = 0
    while idx < len(commandArgs):
        if commandArgs[idx] in ("--rcfile","--init-file", "-O", "+O"):
            idx += 1
        elif commandArgs[idx] == "-c":
            return "COMMAND"
        elif commandArgs[idx].startswith("-"):
            pass
        else:
            return commandArgs[idx]
        idx += 1
    return None

def CshScriptable(execName, commandArgs):
    idx = 0
    while idx < len(commandArgs):
        if commandArgs[idx] == "-b":
            idx += 1
            if idx < len(commandArgs):
                return commandArgs[i]
        elif commandArgs[idx] == "-c":
            return "COMMAND"
        elif commandArgs[idx].startswith("-"):
            pass
        else:
            return commandArgs[idx]
        idx += 1
    return None
