cmake_minimum_required(VERSION 3.20)
project(blaze_mlx LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find MLX
find_package(Python3 COMPONENTS Interpreter)
if(Python3_FOUND)
    execute_process(
        COMMAND ${Python3_EXECUTABLE} -c "import mlx, os; print(os.path.dirname(mlx.__file__))"
        OUTPUT_VARIABLE MLX_ROOT
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
    if(MLX_ROOT)
        set(MLX_INCLUDE_DIR "${MLX_ROOT}/include")
        set(MLX_LIB_DIR "${MLX_ROOT}/lib")
        message(STATUS "Found MLX at: ${MLX_ROOT}")
    endif()
endif()

add_library(blaze_mlx SHARED blaze_mlx.cpp)
target_include_directories(blaze_mlx PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

if(MLX_ROOT AND EXISTS "${MLX_INCLUDE_DIR}")
    target_include_directories(blaze_mlx PRIVATE ${MLX_INCLUDE_DIR})
    target_link_directories(blaze_mlx PRIVATE ${MLX_LIB_DIR})
    target_link_libraries(blaze_mlx PRIVATE mlx)
    target_compile_definitions(blaze_mlx PRIVATE HAS_MLX=1)
    message(STATUS "Building with MLX support")
else()
    message(STATUS "MLX not found, building stub implementation")
    target_compile_definitions(blaze_mlx PRIVATE HAS_MLX=0)
endif()

if(APPLE)
    set_target_properties(blaze_mlx PROPERTIES
        INSTALL_RPATH "@loader_path"
        BUILD_WITH_INSTALL_RPATH TRUE
    )
endif()

install(TARGETS blaze_mlx
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(FILES blaze_mlx.h DESTINATION include)
