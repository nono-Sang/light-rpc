#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>

#include <string>

timespec time_diff(timespec start, timespec end) {
  timespec res;
  if (end.tv_nsec - start.tv_nsec < 0) {
    res.tv_nsec = end.tv_nsec - start.tv_nsec + 1e9;
    res.tv_sec = end.tv_sec - start.tv_sec - 1;
  } else {
    res.tv_nsec = end.tv_nsec - start.tv_nsec;
    res.tv_sec = end.tv_sec - start.tv_sec;
  }
  return res;
}

void time_add(timespec *des, timespec source) {
  if (des->tv_nsec + source.tv_nsec >= 1e9) {
    des->tv_nsec += source.tv_nsec - 1e9;
    des->tv_sec += source.tv_sec + 1;
  } else {
    des->tv_nsec += source.tv_nsec;
    des->tv_sec += source.tv_sec;
  }
}

uint64_t time_avg(timespec t, int num) {
  uint64_t res = (uint64_t)t.tv_nsec + (uint64_t)t.tv_sec * 1e9;
  return res / (uint64_t)num;
}

bool time_compare(timespec left, timespec right) {
  if ((left.tv_sec > right.tv_sec) ||
      (left.tv_sec == right.tv_sec && left.tv_nsec > right.tv_nsec)) {
    return true;
  }
  return false;
}

void GetIpByHostName(std::string &ip, std::string &name) {
  hostent *host = gethostbyname(name.c_str());
  if (host == nullptr) {
    fprintf(stderr, "Failed to gethostbyname.\n");
    exit(EXIT_FAILURE);
  }
  ip = inet_ntoa(*reinterpret_cast<in_addr *>(host->h_addr_list[0]));
}

void GetLocalIp(std::string &local_ip) {
  // Obtain local IP automatically.
  char hostname[20];
  if (gethostname(hostname, sizeof(hostname))) {
    fprintf(stderr, "Failed to get hostname.\n");
    exit(EXIT_FAILURE);
  }
  std::string name = hostname;
  GetIpByHostName(local_ip, name);
}