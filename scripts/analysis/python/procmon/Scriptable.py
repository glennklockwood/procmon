"""procmon/Scriptable contains classes describing different scriptable
   executables and provides methods to attempt to identify the script name"""

import re

def Scriptable(execName, commandArgs):
    localArgs = [ x.strip() for x in commandArgs ]
    if execName == "java":
        return JavaScriptable(execName, localArgs)
    if execName.startswith("python"):
        return PythonScriptable(execName, localArgs)
    if execName.startswith("perl"):
        return PerlScriptable(execName, localArgs)
    if execName.startswith("ruby"):
        return RubyScriptable(execName, localArgs)
    if re.match("^(ba)?sh.*", execName) is not None:
        return BashScriptable(execName, localArgs)
    if re.match("^(t)?csh.*", execName) is not None:
        return CshScriptable(execName, localArgs)
    return None

def JavaScriptable(execName, commandArgs):
    idx = 0
    while idx < len(commandArgs):
        if commandArgs[idx] == "-cp" or commandArgs[idx] == "-classpath":
            # skip next arg
            idx += 1
        elif commandArgs[idx].startswith("-"):
            # skip this arg
            pass
        else:
            # class name (or -jar arg, it turns out that -jar is just a flag to use a jar, doesn't need to be in order)
            if len(commandArgs[idx]) > 0:
                return commandArgs[idx]
            return None
        idx += 1
    return None

def PythonScriptable(execName, commandArgs):
    idx = 0
    while idx < len(commandArgs):
        if commandArgs[idx].startswith('-') and commandArgs[idx].find('c') > 0:
            return "COMMAND"
        elif commandArgs[idx] in ("-m", "-Q", "-W",):
            #skip next arg
            idx += 1
        elif commandArgs[idx].startswith("-"):
            #skip this arg
            pass
        else:
            if len(commandArgs[idx]) > 0:
                return commandArgs[idx]
            return None
        idx += 1
    return None

def PerlScriptable(execName, commandArgs):
    idx = 0
    while idx < len(commandArgs):
        if commandArgs[idx].startswith('-') and (commandArgs[idx].find('e') > 0 or commandArgs[idx].find('E') > 0):
            return "COMMAND";
        elif commandArgs[idx].startswith("-"):
            pass
        else:
            if len(commandArgs[idx]) > 0:
                return commandArgs[idx]
            return None
        idx += 1
    return None

def RubyScriptable(execName, commandArgs):
    idx = 0
    while idx < len(commandArgs):
        if commandArgs[idx].startswith('-') and commandArgs[idx].find('e') > 0:
            return "COMMAND"
        elif commandArgs[idx] in ("-C","-F","-I","-K","-r"):
            idx += 1
        elif commandArgs[idx].startswith("-"):
            pass
        else:
            if len(commandArgs[idx]) > 0:
                return commandArgs[idx]
            return None
        idx += 1
    return None

def BashScriptable(execName, commandArgs):
    idx = 0
    while idx < len(commandArgs):
        if commandArgs[idx].startswith('-') and commandArgs[idx].find('c') > 0:
            return "COMMAND"
        elif commandArgs[idx] in ("--rcfile","--init-file", "-O", "+O"):
            idx += 1
        elif commandArgs[idx].startswith("-"):
            pass
        else:
            if len(commandArgs[idx]) > 0:
                return commandArgs[idx]
            return None
        idx += 1
    return None

def CshScriptable(execName, commandArgs):
    idx = 0
    while idx < len(commandArgs):
        if commandArgs[idx].startswith('-') and commandArgs[idx].find('c') > 0:
            return "COMMAND"
        elif commandArgs[idx].startswith('-') and commandArgs[idx].find('b') > 0:
            idx += 1
            if idx < len(commandArgs):
                if len(commandArgs[idx]) > 0:
                    return commandArgs[i]
                return None
        elif commandArgs[idx].startswith("-"):
            pass
        else:
            if len(commandArgs[idx]) > 0:
                return commandArgs[idx]
            return None
        idx += 1
    return None
