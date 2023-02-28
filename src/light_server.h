#pragma once

#include "src/light_common.h"

#include <boost/unordered_map.hpp>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>

namespace lightrpc {

const int listen_backlog = 200;

enum ServiceOwnership { SERVER_OWNS_SERVICE, SERVER_DOESNT_OWN_SERVICE };

struct ServiceInfo {
    ServiceOwnership ownership;
    google::protobuf::Service* service;
};

struct CallBackArgs {
    uint32_t rpc_id;
    ibv_qp* conn_qp;
    google::protobuf::Message* request;
    google::protobuf::Message* response;
};

class LightServer : public SharedResource {
public:
    LightServer(ResourceConfig config, int num_client);
    void
    AddService(ServiceOwnership ownership, google::protobuf::Service* service);
    void
    BuildAndStart();
    virtual ~LightServer();

private:
    void
    ProcessConnectRequest(rdma_cm_id* conn_id);
    void
    ProcessConnectEstablish(rdma_cm_id* conn_id);
    void
    ProcessDisconnect(rdma_cm_id* conn_id);

    virtual void
    ProcessRecvWorkCompletion(ibv_wc& wc);
    void
    ParseAndRunFunc(ibv_qp* conn_qp, void* addr, uint32_t msg_len);
    void
    ReturnRpcResult(CallBackArgs args);

    // Used to test the normal exit of the server.
    int num_clients_;
    boost::unordered_map<std::string, ServiceInfo> service_map_;

    std::shared_mutex shared_mtx_;
    boost::unordered_map<uint32_t, ibv_qp*> qp_num_map_;
};

} // namespace lightrpc