set(SOURCE_FILES main.cc tool.cc)

add_executable (orctool ${SOURCE_FILES})

target_link_libraries (orctool PUBLIC orc protobuf snappy lz4 zstd z hdfspp_static pthread sasl2 ssl crypto)
