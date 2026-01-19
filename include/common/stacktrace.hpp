#pragma once

#include <execinfo.h>
#include <unistd.h>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace toydb {

inline void printGlibcBacktrace(int signo, siginfo_t *si, [[maybe_unused]] void *context) {
  void *array[100];

  char message[1024];
  ::snprintf(message, sizeof(message), "Process %d received signal: %d. Backtrace:\n", si->si_pid, signo);
  ::write(STDOUT_FILENO, message, ::strnlen(message, sizeof(message)));
  // Not AS-Safe, so just pray we're not in a signal handler that interrupted malloc
  auto size = ::backtrace(array, 100);
  ::backtrace_symbols_fd(array, size, STDOUT_FILENO);
  _exit(EXIT_FAILURE);
}

// Set signal handlers for libunwind stack traces if available
inline void initializeSignalHandlers() {
  struct sigaction action;
  action.sa_sigaction = printGlibcBacktrace;
  action.sa_flags = SA_SIGINFO;
  sigemptyset(&action.sa_mask);
  sigaddset(&action.sa_mask, SIGSEGV);
  ::sigaction(SIGSEGV, &action, nullptr);
  ::sigaction(SIGABRT, &action, nullptr);
  ::sigaction(SIGILL, &action, nullptr);
  ::sigaction(SIGFPE, &action, nullptr);
  ::sigaction(SIGBUS, &action, nullptr);
  ::sigaction(SIGPIPE, &action, nullptr);
  ::sigaction(SIGALRM, &action, nullptr);
  ::sigaction(SIGTERM, &action, nullptr);
}

} // namespace toydb
