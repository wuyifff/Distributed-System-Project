# Define the compiler
CC = g++

# Define compilation options
# CFLAGS = -std=c++11 -Wall -ljsoncpp
CFLAGS = -std=c++11

# Find all cpp files in the current directory
SOURCES = $(wildcard *.cpp)
# Generate corresponding object file list
OBJECTS = $(SOURCES:.cpp=.o)
# Generate corresponding executable file list
EXECUTABLES = $(OBJECTS:.o=.exe)

# Default target, compile all executable files
all: $(EXECUTABLES)

# Generate object files
%.o: %.cpp
	$(CC) $(CFLAGS) -c $< -o $@

# Generate executable files
%.exe: %.o
	$(CC) $(CFLAGS) $< -o $@

# Clean generated object files and executable files
clean:
	rm -f $(OBJECTS) $(EXECUTABLES)
