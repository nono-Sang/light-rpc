#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <string>

inline int64_t GET_CURRENT_MS() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count();
};

inline int64_t GET_CURRENT_NS() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(
		std::chrono::system_clock::now().time_since_epoch()).count();
};

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