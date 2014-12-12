## Discover dependencies

INCLUDE(FindPythonInterp)
FIND_PROGRAM(PYTHON "python")


IF (PYTHON) 
    SET ( SETUP_PY_IN "${CMAKE_CURRENT_SOURCE_DIR}/setup.py.in" )
    SET ( SETUP_PY    "${CMAKE_CURRENT_BINARY_DIR}/setup.py"    )
    SET ( ProcmonOutput ${CMAKE_CURRENT_BINARY_DIR}/procmon_python_inst )
    SET ( PythonInstDir ${CMAKE_INSTALL_PREFIX}/lib/python${PYTHON_VERSION_MAJOR}.${PYTHON_VERSION_MINOR}/site-packages)
    SET ( ENV{PYTHONPATH} ${PythonInstDir} )


    CONFIGURE_FILE ( ${SETUP_PY_IN} ${SETUP_PY} )

    ADD_CUSTOM_COMMAND ( OUTPUT ${ProcmonOutput}
                         COMMAND ${PYTHON}
                         ARGS    setup.py build
                         DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/procmon/*.py
                       )
    ADD_CUSTOM_TARGET ( procmonPython ALL DEPENDS ${ProcmonOutput} )
    INSTALL(CODE "FILE(MAKE_DIRECTORY ${PythonInstDir} )")
    INSTALL(CODE "
        SET(ENV{PYTHONPATH} ${PythonInstDir})
        EXECUTE_PROCESS(COMMAND ${PYTHON} ${SETUP_PY} install --prefix ${CMAKE_INSTALL_PREFIX})
    ")
ENDIF (PYTHON)
