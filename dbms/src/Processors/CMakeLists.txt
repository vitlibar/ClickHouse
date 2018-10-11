if (ENABLE_TESTS)
    add_subdirectory (tests)
endif ()

add_headers_and_sources(dbms ./Executors)
add_headers_and_sources(dbms ./Formats)
add_headers_and_sources(dbms ./FunctionProcessing)
