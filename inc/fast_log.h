#pragma once

#include <error.h>
#include <stdio.h>
#include <stdlib.h>

namespace fast {

#define LOG_INFO(M, ...) \
  fprintf(stderr, "[INFO] (%s:%d) " M "\n", __FILE__, __LINE__, ##__VA_ARGS__)

#define LOG_ERR(M, ...)                          \
  fprintf(stderr,                                \
          "[ERROR] (%s:%d: errno: %s) " M "\n",  \
          __FILE__,                              \
          __LINE__,                              \
          errno == 0 ? "None" : strerror(errno), \
          ##__VA_ARGS__)

#define CHECK(COND)                        \
  do {                                     \
    if (!(COND)) {                         \
      LOG_ERR("Check failure: %s", #COND); \
      exit(EXIT_FAILURE);                  \
    }                                      \
  } while (0);

} // namespace fast