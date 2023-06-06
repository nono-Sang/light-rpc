#pragma once

#include <google/protobuf/service.h>

namespace lightrpc {

class LightController : public google::protobuf::RpcController {
 public:
  LightController() {
    fail_flag_ = false;
    error_info_ = "";
  }

  ~LightController() = default;

  void Reset() override {
    fail_flag_ = false;
    error_info_ = "";
  }

  bool Failed() const override { return fail_flag_; }

  std::string ErrorText() const override { return error_info_; }

  void SetFailed(const std::string &reason) override {
    fail_flag_ = true;
    error_info_ = reason;
  }

  // No need to implement.
  void StartCancel() override{};
  bool IsCanceled() const override { return false; }
  void NotifyOnCancel(google::protobuf::Closure *) override{};

 private:
  bool fail_flag_;
  std::string error_info_;
};

}  // namespace lightrpc