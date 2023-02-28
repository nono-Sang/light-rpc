#pragma once

#include "src/light_protocol.h"

#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>
#include <boost/unordered_map.hpp>
#include <rdma/rdma_cma.h>

#include <thread>
#include <unordered_map>

namespace lightrpc {

struct QueuePairContext {
    std::mutex mtx;
    // The hashmap from rpc_id to msg_mr.
    boost::unordered_map<uint32_t, ibv_mr*> large_mr_map;
};

enum ClientOrServer { CLIENT_FLAG, SERVER_FLAG };

class SharedResource {
public:
    SharedResource(ResourceConfig config);

    void
    CreateControlID();
    void
    BindLocalDevice();
    void
    PrepareSharedQueue();
    void
    PrepareSharedBlockPool();
    void
    StartServiceThreads();
    void
    StartPollerThread();

    // Post a recv request to the srq.
    void
    PostOneRecvRequest(uint64_t recv_addr);

    // Create a QP with the shared queues.
    void
    CreateQueuePair(rdma_cm_id* conn_id);

    // user_func: function used to process recv wc.
    void
    PollWorkCompletion();
    void
    BlockPollWorkCompletion();

    void
    ProcessSendWorkCompletion(ibv_wc& wc);
    virtual void
    ProcessRecvWorkCompletion(ibv_wc& wc) = 0;

    void
    ProcessLargeMessage(ibv_qp* conn_qp, ibv_mr* msg_mr, uint64_t recv_addr,
                        uint32_t msg_len, ClientOrServer flag);

    ibv_mr*
    AllocLargeMsgMR(uint32_t msg_len, ibv_qp* conn_qp);
    ibv_mr*
    GetMemoryRegion(uint32_t rpc_id, ibv_qp* conn_qp);

    virtual ~SharedResource();

protected:
    ResourceConfig config_;
    rdma_cm_id* cm_id_;
    rdma_event_channel* ent_chan_;

    // Shared queue resources.
    ibv_srq* shared_rq_;
    ibv_cq* shared_send_cq_;
    ibv_cq* shared_recv_cq_;
    ibv_comp_channel* send_chan_;
    ibv_comp_channel* recv_chan_;

    // Shared block pool.
    ibv_mr* shared_mr_;
    int num_blocks_;
    boost::lockfree::queue<uint64_t> addr_queue_;

    // Used to execute core tasks.
    int round_robin_idx_; // belongs to one thread
    std::vector<std::thread> thread_pool_;
    std::vector<boost::asio::io_context*> service_pool_;
    std::vector<boost::asio::io_context::work*> work_pool_;

    // Used to recv large message.
    std::unordered_map<std::thread::id, ibv_mr*> local_region_map_;

    int ent_fd_; /* Notify thread to return */
    std::atomic<bool> poller_stop_flag_;
    std::thread wc_poller_;

    std::thread send_wc_processer_;
    boost::asio::io_context processer_service_;
    boost::asio::io_context::work processer_work_;

    /********** Inline functions **********/
    void
    ObtainOneBlock(uint64_t& addr) {
        // Set non-blocking to blocking.
        if(addr_queue_.pop(addr) == false) {
            fprintf(stderr, "Failed to pop, waiting...\n");
            while(addr_queue_.pop(addr) == false) {
            }
            fprintf(stderr, "Succeeded to pop.\n");
        }
    }

    void
    ReturnOneBlock(uint64_t addr) {
        addr_queue_.push(addr);
    }
};

} // namespace lightrpc