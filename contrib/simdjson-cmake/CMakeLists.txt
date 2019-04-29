if (NOT HAVE_AVX2)
    message (WARNING "No AVX2 support")
    return()
endif ()

project(simdjson)
set(PROJECT_SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/../simdjson")

set(SIMDJSON_LIB_NAME simdjson)
set(SIMDJSON_LIB_VERSION "0.0.1" CACHE STRING "simdjson library version")
set(SIMDJSON_LIB_SOVERSION "0.0.1" CACHE STRING "simdjson library soversion")
set(SIMDJSON_BUILD_STATIC ${MAKE_STATIC_LIBRARIES})

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")
find_package(Options)
set(SIMDJSON_OPT_FLAGS "${OPT_FLAGS}" CACHE INTERNAL "" FORCE)

add_subdirectory (../simdjson/src "${CMAKE_CURRENT_BINARY_DIR}/src")

set_target_properties(${SIMDJSON_LIB_NAME} PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
