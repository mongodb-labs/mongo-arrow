cmake_minimum_required(VERSION 3.17)
project(pymongoarrow LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Python COMPONENTS Interpreter Development.Module NumPy REQUIRED)

find_program(CYTHON_EXECUTABLE NAMES cython cython3)
if(NOT CYTHON_EXECUTABLE)
    message(FATAL_ERROR "Cython not found. Ensure cython>=3.0 is in the build environment.")
endif()

# Get PyArrow include and library paths from the build Python interpreter
execute_process(
    COMMAND "${Python_EXECUTABLE}" -c "import pyarrow as pa; print(pa.get_include())"
    OUTPUT_VARIABLE PYARROW_INCLUDE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY
)
execute_process(
    COMMAND "${Python_EXECUTABLE}" -c "import pyarrow as pa; print(';'.join(pa.get_library_dirs()))"
    OUTPUT_VARIABLE PYARROW_LIBRARY_DIRS
    OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY
)
execute_process(
    COMMAND "${Python_EXECUTABLE}" -c "import pyarrow as pa; print(';'.join(pa.get_libraries()))"
    OUTPUT_VARIABLE PYARROW_LIBRARIES
    OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY
)

# Create PyArrow library symlinks on POSIX (required to link against PyPI wheels)
# See https://arrow.apache.org/docs/python/integration/extending.html#building-extensions-against-pypi-wheels
if(NOT WIN32)
    set(_create_symlinks "$ENV{MONGO_CREATE_LIBARROW_SYMLINKS}")
    if(NOT _create_symlinks STREQUAL "0")
        execute_process(
            COMMAND "${Python_EXECUTABLE}" -c "import pyarrow as pa; pa.create_library_symlinks()"
        )
    endif()
endif()

# --- libbson (bson2) detection ---
set(LIBBSON_INSTALL_DIR "$ENV{LIBBSON_INSTALL_DIR}")
if(LIBBSON_INSTALL_DIR)
    get_filename_component(LIBBSON_INSTALL_DIR "${LIBBSON_INSTALL_DIR}" ABSOLUTE)
endif()

if(NOT WIN32)
    find_package(PkgConfig REQUIRED)
    if(LIBBSON_INSTALL_DIR)
        file(GLOB _pc_dirs "${LIBBSON_INSTALL_DIR}/lib*/pkgconfig")
        foreach(_dir ${_pc_dirs})
            set(ENV{PKG_CONFIG_PATH} "${_dir}:$ENV{PKG_CONFIG_PATH}")
        endforeach()
    endif()

    # Read minimum required version from version.py
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/pymongoarrow/version.py" _version_py)
    string(REGEX MATCH "_MIN_LIBBSON_VERSION = \"([^\"]*)\"" _ "${_version_py}")
    set(_min_libbson_version "${CMAKE_MATCH_1}")

    pkg_check_modules(LIBBSON REQUIRED IMPORTED_TARGET "bson2>=${_min_libbson_version}")
else()
    # Windows: resolve bson2 from LIBBSON_INSTALL_DIR directly (no pkg-config)
    if(NOT LIBBSON_INSTALL_DIR)
        message(FATAL_ERROR "LIBBSON_INSTALL_DIR must be set on Windows")
    endif()
    # Discover include dir: libbson 2.x installs to include/bson-X.Y.Z/
    file(GLOB _bson_h "${LIBBSON_INSTALL_DIR}/include/bson-*/bson/bson.h")
    if(NOT _bson_h)
        message(FATAL_ERROR "Could not find bson/bson.h under ${LIBBSON_INSTALL_DIR}/include")
    endif()
    list(GET _bson_h 0 _bson_h_first)
    get_filename_component(_bson_h_dir "${_bson_h_first}" DIRECTORY)
    get_filename_component(LIBBSON_INCLUDE_DIRS "${_bson_h_dir}" DIRECTORY)
    # bson2.dll.lib is the DLL import library (defines __imp_bson_* symbols).
    # bson2.lib is the static library — do NOT link that; it won't resolve dllimport symbols.
    file(GLOB _bson2_libs
        "${LIBBSON_INSTALL_DIR}/lib/bson2.dll.lib"
        "${LIBBSON_INSTALL_DIR}/lib/*/bson2.dll.lib"
    )
    if(NOT _bson2_libs)
        message(FATAL_ERROR "Could not find bson2.dll.lib under ${LIBBSON_INSTALL_DIR}/lib")
    endif()
    list(GET _bson2_libs 0 LIBBSON_LINK_LIBS)
endif()

# --- Cython compilation ---
set(_cython_source "${CMAKE_CURRENT_SOURCE_DIR}/pymongoarrow/lib.pyx")
set(_cython_output "${CMAKE_CURRENT_BINARY_DIR}/lib.cpp")

add_custom_command(
    OUTPUT "${_cython_output}"
    COMMAND "${CYTHON_EXECUTABLE}"
        --cplus
        -3
        "--include-dir" "${CMAKE_CURRENT_SOURCE_DIR}/pymongoarrow"
        "--output-file" "${_cython_output}"
        "${_cython_source}"
    DEPENDS
        "${_cython_source}"
        "${CMAKE_CURRENT_SOURCE_DIR}/pymongoarrow/libbson.pxd"
        "${CMAKE_CURRENT_SOURCE_DIR}/pymongoarrow/libarrow.pxd"
    COMMENT "Cythonizing lib.pyx"
    VERBATIM
)

# --- Build the Python extension module ---
Python_add_library(lib MODULE WITH_SOABI "${_cython_output}")

target_include_directories(lib PRIVATE
    "${Python_NumPy_INCLUDE_DIRS}"
    "${PYARROW_INCLUDE_DIR}"
)

if(WIN32)
    target_include_directories(lib PRIVATE "${LIBBSON_INCLUDE_DIRS}")
    target_link_libraries(lib PRIVATE "${LIBBSON_LINK_LIBS}")
else()
    # PkgConfig::LIBBSON carries include dirs, library dirs, and link flags
    target_link_libraries(lib PRIVATE PkgConfig::LIBBSON)
endif()

foreach(_dir ${PYARROW_LIBRARY_DIRS})
    target_link_directories(lib PRIVATE "${_dir}")
endforeach()
target_link_libraries(lib PRIVATE ${PYARROW_LIBRARIES})

# Set RPATH so the extension can find bson2 at runtime after installation
if(APPLE)
    set_target_properties(lib PROPERTIES
        INSTALL_RPATH "@loader_path"
        BUILD_WITH_INSTALL_RPATH TRUE
    )
elseif(UNIX)
    set_target_properties(lib PROPERTIES
        INSTALL_RPATH "$ORIGIN"
        BUILD_WITH_INSTALL_RPATH TRUE
    )
endif()

# --- Installation ---
install(TARGETS lib DESTINATION pymongoarrow)

# Install Cython declaration files so downstream packages can cimport from pymongoarrow
install(FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/pymongoarrow/libbson.pxd"
    "${CMAKE_CURRENT_SOURCE_DIR}/pymongoarrow/libarrow.pxd"
    DESTINATION pymongoarrow
)

# Copy the bson2 shared library into the package directory for runtime loading
# Skip when MONGO_NO_COPY_LIBBSON is set (e.g. conda builds provide bson2 separately)
if(NOT "$ENV{MONGO_NO_COPY_LIBBSON}" AND LIBBSON_INSTALL_DIR)
    if(APPLE)
        # Find the SOVERSION symlink (e.g. libbson2.2.dylib) — the name the
        # extension resolves via @rpath at runtime. Resolve it to the real file
        # and install with RENAME so the destination is a regular file, not a
        # broken symlink.
        file(GLOB _all_dylibs "${LIBBSON_INSTALL_DIR}/lib*/libbson2.*.dylib")
        foreach(_f ${_all_dylibs})
            get_filename_component(_fname "${_f}" NAME)
            if(_fname MATCHES "^libbson2\\.[0-9]+\\.dylib$")
                get_filename_component(_libbson_real "${_f}" REALPATH)
                install(FILES "${_libbson_real}" DESTINATION pymongoarrow RENAME "${_fname}")
            endif()
        endforeach()
    elseif(UNIX)
        # Find the SOVERSION symlink (e.g. libbson2.so.2) and install its real target.
        file(GLOB _all_so "${LIBBSON_INSTALL_DIR}/lib*/libbson2.so.*")
        foreach(_f ${_all_so})
            get_filename_component(_fname "${_f}" NAME)
            if(_fname MATCHES "^libbson2\\.so\\.[0-9]+$")
                get_filename_component(_libbson_real "${_f}" REALPATH)
                install(FILES "${_libbson_real}" DESTINATION pymongoarrow RENAME "${_fname}")
            endif()
        endforeach()
    elseif(WIN32)
        # MSVC multi-config installs to bin/<Config>/bson2.dll; Ninja to bin/bson2.dll
        file(GLOB _libbson_bin
            "${LIBBSON_INSTALL_DIR}/bin/bson2.dll"
            "${LIBBSON_INSTALL_DIR}/bin/*/bson2.dll"
        )
        if(_libbson_bin)
            list(GET _libbson_bin 0 _first_dll)
            install(FILES "${_first_dll}" DESTINATION pymongoarrow RENAME bson2.dll)
        endif()
    endif()
endif()
