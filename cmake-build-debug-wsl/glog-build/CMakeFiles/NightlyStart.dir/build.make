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

# Utility rule file for NightlyStart.

# Include any custom commands dependencies for this target.
include glog-build/CMakeFiles/NightlyStart.dir/compiler_depend.make

# Include the progress variables for this target.
include glog-build/CMakeFiles/NightlyStart.dir/progress.make

glog-build/CMakeFiles/NightlyStart:
	cd /home/tauh/db/minisql/cmake-build-debug-wsl/glog-build && /usr/bin/ctest -D NightlyStart

NightlyStart: glog-build/CMakeFiles/NightlyStart
NightlyStart: glog-build/CMakeFiles/NightlyStart.dir/build.make
.PHONY : NightlyStart

# Rule to build all files generated by this target.
glog-build/CMakeFiles/NightlyStart.dir/build: NightlyStart
.PHONY : glog-build/CMakeFiles/NightlyStart.dir/build

glog-build/CMakeFiles/NightlyStart.dir/clean:
	cd /home/tauh/db/minisql/cmake-build-debug-wsl/glog-build && $(CMAKE_COMMAND) -P CMakeFiles/NightlyStart.dir/cmake_clean.cmake
.PHONY : glog-build/CMakeFiles/NightlyStart.dir/clean

glog-build/CMakeFiles/NightlyStart.dir/depend:
	cd /home/tauh/db/minisql/cmake-build-debug-wsl && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/tauh/db/minisql /home/tauh/db/minisql/thirdparty/glog /home/tauh/db/minisql/cmake-build-debug-wsl /home/tauh/db/minisql/cmake-build-debug-wsl/glog-build /home/tauh/db/minisql/cmake-build-debug-wsl/glog-build/CMakeFiles/NightlyStart.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : glog-build/CMakeFiles/NightlyStart.dir/depend

