cmake_minimum_required(VERSION 3.20)

project(CloudShard LANGUAGES CXX)

# ============================================================
# C++ CONFIGURATION
# ============================================================

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ============================================================
# DEPENDENCIES
# ============================================================

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(CURL REQUIRED)

# ============================================================
# DIRECTORIES
# ============================================================

set(SRC_DIR
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

set(PROTO_DIR
    ${CMAKE_CURRENT_SOURCE_DIR}/proto
)

set(GENERATED_DIR
    ${CMAKE_CURRENT_BINARY_DIR}/generated
)

file(MAKE_DIRECTORY ${GENERATED_DIR})

# ============================================================
# AUTOMATIC C++ SOURCE DISCOVERY
# ============================================================

# Find every .cpp file under src/.
#
# IMPORTANT:
# Each .cpp file found here is treated as a separate executable.
#
# Example:
#
#   src/main.cpp
#       -> Outputs/main
#
#   src/createDB.cpp
#       -> Outputs/createDB
#
#   src/testConnection.cpp
#       -> Outputs/testConnection

file(GLOB_RECURSE CPP_SOURCES
    CONFIGURE_DEPENDS
    "${SRC_DIR}/*.cpp"
)

# ============================================================
# HEADER DISCOVERY
# ============================================================

# Headers are NOT executables.
# They are included by the .cpp files that need them.

file(GLOB_RECURSE CPP_HEADERS
    CONFIGURE_DEPENDS
    "${SRC_DIR}/*.hpp"
    "${SRC_DIR}/*.h"
)

# ============================================================
# AUTOMATIC PROTO FILE DISCOVERY
# ============================================================

# Find every .proto file under proto/.
#
# This supports:
#
#   proto/shard.proto
#   proto/node.proto
#   proto/communication/stream.proto
#   proto/anything/whatever.proto

file(GLOB_RECURSE PROTO_FILES
    CONFIGURE_DEPENDS
    "${PROTO_DIR}/*.proto"
)

set(GENERATED_PROTO_SOURCES)
set(GENERATED_PROTO_HEADERS)

# ============================================================
# PROTOBUF + GRPC CODE GENERATION
# ============================================================

foreach(PROTO_FILE IN LISTS PROTO_FILES)

    # Get path relative to proto/.

    file(RELATIVE_PATH PROTO_RELATIVE_PATH
        "${PROTO_DIR}"
        "${PROTO_FILE}"
    )

    # Remove .proto extension.

    string(REGEX REPLACE "\\.proto$" ""
        PROTO_RELATIVE_PATH
        "${PROTO_RELATIVE_PATH}"
    )

    # --------------------------------------------------------
    # Generated files
    # --------------------------------------------------------

    set(PB_CC
        "${GENERATED_DIR}/${PROTO_RELATIVE_PATH}.pb.cc"
    )

    set(PB_H
        "${GENERATED_DIR}/${PROTO_RELATIVE_PATH}.pb.h"
    )

    set(GRPC_CC
        "${GENERATED_DIR}/${PROTO_RELATIVE_PATH}.grpc.pb.cc"
    )

    set(GRPC_H
        "${GENERATED_DIR}/${PROTO_RELATIVE_PATH}.grpc.pb.h"
    )

    # --------------------------------------------------------
    # Create required output directory
    # --------------------------------------------------------

    get_filename_component(OUTPUT_DIR
        "${PB_CC}"
        DIRECTORY
    )

    file(MAKE_DIRECTORY "${OUTPUT_DIR}")

    # --------------------------------------------------------
    # Generate protobuf + gRPC files
    # --------------------------------------------------------

    add_custom_command(
        OUTPUT
            "${PB_CC}"
            "${PB_H}"
            "${GRPC_CC}"
            "${GRPC_H}"

        COMMAND
            $<TARGET_FILE:protobuf::protoc>
            --proto_path=${PROTO_DIR}
            --cpp_out=${GENERATED_DIR}
            --grpc_out=${GENERATED_DIR}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            ${PROTO_RELATIVE_PATH}.proto

        WORKING_DIRECTORY
            "${PROTO_DIR}"

        DEPENDS
            "${PROTO_FILE}"
            protobuf::protoc
            gRPC::grpc_cpp_plugin

        COMMENT
            "Generating protobuf/gRPC files for ${PROTO_RELATIVE_PATH}.proto"

        VERBATIM
    )

    # --------------------------------------------------------
    # Store generated files
    # --------------------------------------------------------

    list(APPEND GENERATED_PROTO_SOURCES
        "${PB_CC}"
        "${GRPC_CC}"
    )

    list(APPEND GENERATED_PROTO_HEADERS
        "${PB_H}"
        "${GRPC_H}"
    )

endforeach()

# ============================================================
# EXECUTABLES
# ============================================================

# Every .cpp file becomes its own executable.
#
# Example:
#
#   main.cpp          -> main
#   createDB.cpp      -> createDB
#   testConnection.cpp -> testConnection
#
# Headers and generated protobuf/gRPC sources are linked
# into every executable.

foreach(CPP_FILE IN LISTS CPP_SOURCES)

    # Get filename without extension.
    get_filename_component(EXECUTABLE_NAME
        "${CPP_FILE}"
        NAME_WE
    )

    message(STATUS
        "Creating executable: ${EXECUTABLE_NAME}"
    )

    add_executable(${EXECUTABLE_NAME}
        "${CPP_FILE}"
        ${CPP_HEADERS}
        ${GENERATED_PROTO_SOURCES}
        ${GENERATED_PROTO_HEADERS}
    )

    # --------------------------------------------------------
    # Include directories
    # --------------------------------------------------------

    target_include_directories(${EXECUTABLE_NAME}
        PRIVATE
            "${SRC_DIR}"
            "${GENERATED_DIR}"
    )

    # --------------------------------------------------------
    # Link gRPC + Protobuf
    # --------------------------------------------------------

    target_link_libraries(${EXECUTABLE_NAME}
        PRIVATE
            gRPC::grpc++
            protobuf::libprotobuf
            SQLite::SQLite3
            CURL::libcurl
    )

    # --------------------------------------------------------
    # Output directory
    # --------------------------------------------------------

    set_target_properties(${EXECUTABLE_NAME} PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY
            "${CMAKE_CURRENT_SOURCE_DIR}/Outputs"
    )

endforeach()

# ============================================================
# BUILD INFORMATION
# ============================================================

message(STATUS "========================================")
message(STATUS "CloudShard CMake configuration")
message(STATUS "========================================")

message(STATUS "Source directory:    ${SRC_DIR}")
message(STATUS "Proto directory:     ${PROTO_DIR}")
message(STATUS "Generated directory: ${GENERATED_DIR}")

message(STATUS "")
message(STATUS "C++ sources:")
foreach(CPP_FILE IN LISTS CPP_SOURCES)
    message(STATUS "  ${CPP_FILE}")
endforeach()

message(STATUS "")
message(STATUS "C++ headers:")
foreach(CPP_HEADER IN LISTS CPP_HEADERS)
    message(STATUS "  ${CPP_HEADER}")
endforeach()

message(STATUS "")
message(STATUS "Proto files:")
foreach(PROTO_FILE IN LISTS PROTO_FILES)
    message(STATUS "  ${PROTO_FILE}")
endforeach()

message(STATUS "========================================")