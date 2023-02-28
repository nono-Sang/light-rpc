#pragma once

#include "src/light_common.h"

#include <boost/unordered_map.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

namespace lightrpc {

const int timeout_in_ms = 1000;

struct RpcCallInfo {
    std::promise<bool>& prom;
    google::protobuf::Message* response;
    google::protobuf::Closure* callback;
};

class LightChannel;
class ClientSharedResource : public SharedResource {
    friend class LightChannel;

public:
    ClientSharedResource(ResourceConfig config) : SharedResource(config) {}
    virtual ~ClientSharedResource() {}

private:
    virtual void
    ProcessRecvWorkCompletion(ibv_wc& wc);
    void
    ParseAndRunAfter(LightChannel* client, void* addr, uint32_t msg_len);

    std::shared_mutex shared_mtx_;
    boost::unordered_map<uint32_t, LightChannel*> client_map_;
};

class LightChannel : public google::protobuf::RpcChannel {
    friend class ClientSharedResource;

public:
    LightChannel(std::string dest_ip, uint16_t dest_port,
                 ClientSharedResource* shared_res);
    void
    CallMethod(const google::protobuf::MethodDescriptor* method,
               google::protobuf::RpcController* controller,
               const google::protobuf::Message* request,
               google::protobuf::Message* response,
               google::protobuf::Closure* done) override;
    virtual ~LightChannel();

private:
    ClientSharedResource* shared_res_;
    rdma_cm_id* conn_id_;
    rdma_event_channel* ent_chan_;

    std::mutex id_mtx_;
    uint32_t rpc_id_;
    std::mutex map_mtx_;
    boost::unordered_map<uint32_t, RpcCallInfo> callinfo_map_;
};

} // namespace lightrpc