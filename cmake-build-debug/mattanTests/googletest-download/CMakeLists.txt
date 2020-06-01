cmake_minimum_required(VERSION 2.8.2)

project(googletest-download NONE)

include(ExternalProject)
ExternalProject_Add(googletest
  GIT_REPOSITORY    https://github.com/google/googletest.git
  GIT_TAG           master
  SOURCE_DIR        "/cs/usr/ophirhan/CLionProjects/os_ex3_2020/cmake-build-debug/mattanTests/googletest-src"
  BINARY_DIR        "/cs/usr/ophirhan/CLionProjects/os_ex3_2020/cmake-build-debug/mattanTests/googletest-build"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND     ""
  INSTALL_COMMAND   ""
  TEST_COMMAND      ""
)
