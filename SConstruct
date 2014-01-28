import os
procmonVersion="2.5"
env = Environment()
options = Variables('config.h')

try:
	env.Append(RABBITMQ_C_DIR = os.environ['RABBITMQ_C_DIR'])
except KeyError:
	options.Add(PathVariable('RABBITMQ_C_DIR', 'Path to rabbitmq-c installation', '/usr/local', PathVariable.PathIsDir))

try:
	env.Append(HDF5_DIR = os.environ['HDF5_DIR'])
except KeyError:
	options.Add(PathVariable('HDF5_DIR', 'Path to hdf5 installation', '/usr/local', PathVariable.PathIsDir))

options.Add(PathVariable('PREFIX', 'Installation directory', '/usr/local', PathVariable.PathIsDir))
options.Update(env)

Help(options.GenerateHelpText(env))
conf = Configure(env)
if not conf.CheckCXX():
	print('!! Your compiler and/or environment is not correctly configured.')
	Exit(1)

env = conf.Finish()
