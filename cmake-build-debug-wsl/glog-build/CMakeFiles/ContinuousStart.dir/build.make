# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.22

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Disable VCS-based implicit rules.
% : %,v

# Disable VCS-based implicit rules.
% : RCS/%

# Disable VCS-based implicit rules.
% : RCS/%,v

# Disable VCS-based implicit rules.
% : SCCS/s.%

# Disable VCS-based implicit rules.
% : s.%

.SUFFIXES: .hpux_make_needs_suffix_list

# Command-line flag to silence nested $(MAKE).
$(VERBOSE)MAKESILENT = -s

#Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E rm -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/tauh/db/minisql

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/tauh/db/minisql/cmake-build-debug-wsl

# Utility rule file for ContinuousStart.

# Include any custom commands dependencies for this target.
include glog-build/CMakeFiles/ContinuousStart.dir/compiler_depend.make

# Include the progress variables for this target.
include glog-build/CMakeFiles/ContinuousStart.dir/progress.make

glog-build/CMakeFiles/ContinuousStart:
	cd /home/tauh/db/minisql/cmake-build-debug-wsl/glog-build && /usr/bin/ctest -D ContinuousStart

ContinuousStart: glog-build/CMakeFiles/ContinuousStart
ContinuousStart: glog-build/CMakeFiles/ContinuousStart.dir/build.make
.PHONY : ContinuousStart

# Rule to build all files generated by this target.
glog-build/CMakeFiles/ContinuousStart.dir/build: ContinuousStart
.PHONY : glog-build/CMakeFiles/ContinuousStart.dir/build

glog-build/CMakeFiles/ContinuousStart.dir/clean:
	cd /home/tauh/db/minisql/cmake-build-debug-wsl/glog-build && $(CMAKE_COMMAND) -P CMakeFiles/ContinuousStart.dir/cmake_clean.cmake
.PHONY : glog-build/CMakeFiles/ContinuousStart.dir/clean

glog-build/CMakeFiles/ContinuousStart.dir/depend:
	cd /home/tauh/db/minisql/cmake-build-debug-wsl && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/tauh/db/minisql /home/tauh/db/minisql/thirdparty/glog /home/tauh/db/minisql/cmake-build-debug-wsl /home/tauh/db/minisql/cmake-build-debug-wsl/glog-build /home/tauh/db/minisql/cmake-build-debug-wsl/glog-build/CMakeFiles/ContinuousStart.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : glog-build/CMakeFiles/ContinuousStart.dir/depend

