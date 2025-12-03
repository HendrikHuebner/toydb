.PHONY: debug relwithdebug asan clean all help test test-verbose test-%

BUILD_DIR := build

CC := clang
CXX := clang++
LINKER := mold

CMAKE_FLAGS := -G Ninja \
	-DCMAKE_C_COMPILER=$(CC) \
	-DCMAKE_CXX_COMPILER=$(CXX) \
	-DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
	-DTDB_USE_LINKER=$(LINKER)

all: relwithdebug

debug:
	@echo "Configuring CMake for Debug build..."
	cmake -B $(BUILD_DIR) -S . \
		$(CMAKE_FLAGS) \
		-DCMAKE_BUILD_TYPE=Debug
	@echo "Building Debug..."
	cmake --build $(BUILD_DIR) --config Debug

relwithdebug:
	@echo "Configuring CMake for RelWithDebInfo build..."
	cmake -B $(BUILD_DIR) -S . \
		$(CMAKE_FLAGS) \
		-DCMAKE_BUILD_TYPE=RelWithDebInfo
	@echo "Building RelWithDebInfo..."
	cmake --build $(BUILD_DIR) --config RelWithDebInfo

asan:
	@echo "Configuring CMake for AddressSanitizer build..."
	cmake -B $(BUILD_DIR) -S . \
		$(CMAKE_FLAGS) \
		-DCMAKE_BUILD_TYPE=Debug \
		-DENABLE_ASAN=ON
	@echo "Building with AddressSanitizer..."
	cmake --build $(BUILD_DIR) --config Debug

clean:
	@echo "Cleaning build directory..."
	rm -rf $(BUILD_DIR)

test:
	@echo "Running all tests..."
	@cd $(BUILD_DIR) && ctest --output-on-failure

test-verbose:
	@echo "Running all tests (verbose)..."
	@cd $(BUILD_DIR) && ctest --verbose --output-on-failure

test-%:
	@echo "Running test: $*"
	@cd $(BUILD_DIR) && ctest --output-on-failure -R "^$*$$"

