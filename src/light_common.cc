#include "src/light_common.h"
#include "src/light_api.h"

#include <mimalloc/mimalloc.h>

namespace lightrpc {

SharedResource::SharedResource(ResourceConfig config)
    : config_(config), num_blocks_(config.block_pool_size / msg_threshold),
      round_robin_idx_(0), addr_queue_(num_blocks_),
      processer_work_(processer_service_) {
    CreateControlID();
    BindLocalDevice();
    PrepareSharedQueue();
    PrepareSharedBlockPool();
    StartServiceThreads();
    StartPollerThread();
}

void
SharedResource::CreateControlID() {
    // Create event channel and cm id.
    ent_chan_ = rdma_create_event_channel();
    CHECK(ent_chan_ != nullptr);
    CHECK(rdma_create_id(ent_chan_, &cm_id_, nullptr, RDMA_PS_TCP) == 0);
}

void
SharedResource::BindLocalDevice() {
    sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(config_.local_ip.c_str());
    if(config_.local_port >= 0)
        addr.sin_port = htons(config_.local_port);
    CHECK(rdma_bind_addr(cm_id_, reinterpret_cast<sockaddr*>(&addr)) == 0);
    // Check whether the verb and pd have been obtained.
    CHECK(cm_id_->verbs != nullptr);
    CHECK(cm_id_->pd != nullptr);
    // QueryDeviceAttribute(cm_id_->verbs);
}

void
SharedResource::PrepareSharedQueue() {
    ibv_srq_init_attr srq_init_attr;
    memset(&srq_init_attr, 0, sizeof(srq_init_attr));
    srq_init_attr.attr.max_sge = srq_max_sge;
    srq_init_attr.attr.max_wr = srq_max_wr;
    shared_rq_ = ibv_create_srq(cm_id_->pd, &srq_init_attr);
    CHECK(shared_rq_ != nullptr);

    send_chan_ = ibv_create_comp_channel(cm_id_->verbs);
    CHECK(send_chan_ != nullptr);
    recv_chan_ = ibv_create_comp_channel(cm_id_->verbs);
    CHECK(recv_chan_ != nullptr);

    shared_send_cq_ =
            ibv_create_cq(cm_id_->verbs, min_cqe_num, nullptr, send_chan_, 0);
    CHECK(shared_send_cq_ != nullptr);
    shared_recv_cq_ =
            ibv_create_cq(cm_id_->verbs, min_cqe_num, nullptr, recv_chan_, 0);
    CHECK(shared_recv_cq_ != nullptr);
}

void
SharedResource::PrepareSharedBlockPool() {
    void* addr_ptr = mi_malloc(config_.block_pool_size);
    CHECK(addr_ptr != nullptr);
    shared_mr_ = ibv_reg_mr(cm_id_->pd, addr_ptr, config_.block_pool_size,
                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    CHECK(shared_mr_ != nullptr);

    uint64_t mr_addr = reinterpret_cast<uint64_t>(addr_ptr);
    for(int i = 0; i < num_blocks_; i++) {
        addr_queue_.unsynchronized_push(mr_addr + i * msg_threshold);
    }
}

void
SharedResource::StartServiceThreads() {
    thread_pool_.resize(config_.num_threads);
    service_pool_.resize(config_.num_threads);
    work_pool_.resize(config_.num_threads);

    for(int i = 0; i < config_.num_threads; i++) {
        service_pool_[i] = new boost::asio::io_context;
        work_pool_[i] = new boost::asio::io_context::work(*service_pool_[i]);
        thread_pool_[i] = std::thread([this, i] { service_pool_[i]->run(); });
        std::thread::id corr_id = thread_pool_[i].get_id();

        void* addr_ptr = mi_malloc(max_threshold);
        CHECK(addr_ptr != nullptr);
        ibv_mr* local_mr =
                ibv_reg_mr(cm_id_->pd, addr_ptr, max_threshold,
                           IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
        CHECK(local_mr != nullptr);
        local_region_map_.insert({corr_id, local_mr});
    }

    // Use a thread to deregis memory region.
    send_wc_processer_ = std::thread([this] { processer_service_.run(); });
}

void
SharedResource::StartPollerThread() {
    poller_stop_flag_ = false;
    wc_poller_ = std::thread([this] {
        if(config_.poll_mode == BUSY_POLLING)
            PollWorkCompletion();
        else
            BlockPollWorkCompletion();
    });
}

void
SharedResource::PostOneRecvRequest(uint64_t recv_addr) {
    ibv_sge recv_sg = {.addr = recv_addr,
                       .length = msg_threshold,
                       .lkey = shared_mr_->lkey};
    ibv_recv_wr recv_wr;
    ibv_recv_wr* recv_bad_wr = nullptr;
    memset(&recv_wr, 0, sizeof(recv_wr));
    // Store recv address in wr_id field.
    recv_wr.wr_id = recv_addr;
    recv_wr.num_sge = 1;
    recv_wr.sg_list = &recv_sg;
    CHECK(ibv_post_srq_recv(shared_rq_, &recv_wr, &recv_bad_wr) == 0);
}

void
SharedResource::CreateQueuePair(rdma_cm_id* conn_id) {
    ibv_qp_init_attr qp_attr;
    memset(&qp_attr, 0, sizeof(qp_attr));
    qp_attr.sq_sig_all = 0;
    qp_attr.qp_type = IBV_QPT_RC;
    qp_attr.srq = shared_rq_;
    qp_attr.send_cq = shared_send_cq_;
    qp_attr.recv_cq = shared_recv_cq_;
    qp_attr.cap.max_send_wr = max_send_wr;
    qp_attr.cap.max_send_sge = max_send_sge;
    qp_attr.cap.max_inline_data = max_inline_data;
    CHECK(rdma_create_qp(conn_id, conn_id->pd, &qp_attr) == 0);

    // Create queue pair context (used for large message).
    conn_id->qp->qp_context = reinterpret_cast<void*>(new QueuePairContext);
}

void
SharedResource::PollWorkCompletion() {
    int send_res = 0, recv_res = 0;
    ibv_wc send_wc, recv_wc;
    while(true) {
        if(poller_stop_flag_)
            return;
        send_res = ibv_poll_cq(shared_send_cq_, 1, &send_wc);
        CHECK(send_res >= 0);
        if(send_res == 1) {
            CHECK(send_wc.status == IBV_WC_SUCCESS);
            ProcessSendWorkCompletion(send_wc);
            memset(&send_wc, 0, sizeof(send_wc));
        }
        recv_res = ibv_poll_cq(shared_recv_cq_, 1, &recv_wc);
        CHECK(recv_res >= 0);
        if(recv_res == 1) {
            CHECK(recv_wc.status == IBV_WC_SUCCESS);
            ProcessRecvWorkCompletion(recv_wc);
            memset(&recv_wc, 0, sizeof(recv_wc));
        }
    }
}

void
SharedResource::BlockPollWorkCompletion() {
    // Next completion whether it is solicited or not.
    CHECK(ibv_req_notify_cq(shared_send_cq_, 0) == 0);
    CHECK(ibv_req_notify_cq(shared_recv_cq_, 0) == 0);

    ent_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    int send_fd = send_chan_->fd;
    int send_flags = fcntl(send_fd, F_GETFL);
    CHECK(fcntl(send_fd, F_SETFL, send_flags | O_NONBLOCK) >= 0);
    int recv_fd = recv_chan_->fd;
    int recv_flags = fcntl(recv_fd, F_GETFL);
    CHECK(fcntl(recv_fd, F_SETFL, recv_flags | O_NONBLOCK) >= 0);

    pollfd arr[3];
    arr[0] = {.fd = ent_fd_, .events = POLLIN};
    arr[1] = {.fd = send_fd, .events = POLLIN};
    arr[2] = {.fd = recv_fd, .events = POLLIN};

    auto ProcessPollIn = [this](ibv_comp_channel* comp_chan,
                                ibv_cq* shared_cq) {
        ibv_cq* ev_cq = nullptr;
        void* ev_ctx = nullptr;
        CHECK(ibv_get_cq_event(comp_chan, &ev_cq, &ev_ctx) == 0);
        CHECK(ev_cq == shared_cq);
        ibv_ack_cq_events(ev_cq, 1);
        CHECK(ibv_req_notify_cq(shared_cq, 0) == 0);

        int res = 0;
        ibv_wc wc;
        while(true) {
            res = ibv_poll_cq(shared_cq, 1, &wc);
            if(res == 0)
                break;
            CHECK(res == 1 && wc.status == IBV_WC_SUCCESS);
            if (shared_cq == shared_send_cq_) 
                ProcessSendWorkCompletion(wc);
            else 
                ProcessRecvWorkCompletion(wc);
            memset(&wc, 0, sizeof(wc));
        }
    };

    while(true) {
        int num = poll(arr, 3, -1);
        CHECK(num > 0);
        if(arr[0].revents & POLLIN)
            return;
        if(arr[1].revents & POLLIN)
            ProcessPollIn(send_chan_, shared_send_cq_);
        if(arr[2].revents & POLLIN)
            ProcessPollIn(recv_chan_, shared_recv_cq_);
    }
}

void
SharedResource::ProcessSendWorkCompletion(ibv_wc& wc) {
    if(wc.opcode == IBV_WC_SEND) {
        ReturnOneBlock(wc.wr_id);
    } else {
        CHECK(wc.opcode == IBV_WC_RDMA_WRITE);
        uint64_t wr_id = wc.wr_id;
        processer_service_.post([this, wr_id] {
            ibv_mr* mr_ptr = reinterpret_cast<ibv_mr*>(wr_id);
            void* mr_addr_ptr = mr_ptr->addr;
            CHECK(ibv_dereg_mr(mr_ptr) == 0);
            mi_free(mr_addr_ptr);
        });
    }
}

void
SharedResource::ProcessLargeMessage(ibv_qp* conn_qp, ibv_mr* msg_mr,
                                    uint64_t recv_addr, uint32_t msg_len,
                                    ClientOrServer flag) {
    // Get the remote info (target addr and rkey).
    RemoteInfo remote_info;
    if(msg_len >= max_threshold) {
        RemoteInfoWithId info_with_id;
        memcpy(&info_with_id, reinterpret_cast<char*>(recv_addr),
               sizeof(info_with_id));
        auto qp_context =
                reinterpret_cast<QueuePairContext*>(conn_qp->qp_context);
        std::unique_lock<std::mutex> locker(qp_context->mtx);
        qp_context->large_mr_map.insert({info_with_id.rpc_id, msg_mr});
        locker.unlock();
        remote_info = {.remote_key = info_with_id.remote_key,
                       .remote_addr = info_with_id.remote_addr};
    } else {
        memcpy(&remote_info, reinterpret_cast<char*>(recv_addr),
               sizeof(remote_info));
    }

    // For client, only return is required.
    // For server, post for next rpc call (reuse address).
    if(flag == CLIENT_FLAG)
        ReturnOneBlock(recv_addr);
    else
        PostOneRecvRequest(recv_addr);

    // Write the memory region info to client.
    if(msg_len >= max_threshold) {
        // Post recv-req for write-with-imm_data.
        uint64_t addr = 0;
        ObtainOneBlock(addr);
        PostOneRecvRequest(addr);
        WriteMemoryRegionInfo(conn_qp, msg_mr, remote_info);
    } else {
        char* ptr_tag = reinterpret_cast<char*>(msg_mr->addr) + msg_len;
        memset(ptr_tag, '0', 1);
        WriteMemoryRegionInfo(conn_qp, msg_mr, remote_info);

        // Judge the end of write operation by polling.
        while(*ptr_tag != '1') {
        }
    }
}

ibv_mr*
SharedResource::AllocLargeMsgMR(uint32_t msg_len, ibv_qp* conn_qp) {
    ibv_mr* msg_mr = nullptr;
    if(msg_len >= max_threshold) {
        void* msg_addr = mi_malloc(msg_len);
        CHECK(msg_addr != nullptr);
        msg_mr = ibv_reg_mr(conn_qp->pd, msg_addr, msg_len,
                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
        CHECK(msg_mr != nullptr);
    } else {
        auto thread_id = std::this_thread::get_id();
        auto iter = local_region_map_.find(thread_id);
        CHECK(iter != local_region_map_.end());
        msg_mr = iter->second;
    }
    return msg_mr;
}

ibv_mr*
SharedResource::GetMemoryRegion(uint32_t rpc_id, ibv_qp* conn_qp) {
    auto qp_context = reinterpret_cast<QueuePairContext*>(conn_qp->qp_context);
    ibv_mr* msg_mr = nullptr;
    std::unique_lock<std::mutex> locker(qp_context->mtx);
    auto iter = qp_context->large_mr_map.find(rpc_id);
    CHECK(iter != qp_context->large_mr_map.end());
    msg_mr = iter->second;
    qp_context->large_mr_map.erase(rpc_id);
    locker.unlock();
    return msg_mr;
}

SharedResource::~SharedResource() {
    for(int i = 0; i < config_.num_threads; i++) {
        service_pool_[i]->stop();
        thread_pool_[i].join();
        delete work_pool_[i];
        delete service_pool_[i];
    }

    if(config_.poll_mode == BUSY_POLLING) {
        poller_stop_flag_ = true;
        wc_poller_.join();
    } else {
        uint64_t val = 1;
        CHECK(write(ent_fd_, &val, sizeof(uint64_t)) != -1);
        wc_poller_.join();
    }

    processer_service_.stop();
    send_wc_processer_.join();

    void* addr_ptr = shared_mr_->addr;
    CHECK(ibv_dereg_mr(shared_mr_) == 0);
    mi_free(addr_ptr);

    for(auto& kv : local_region_map_) {
        void* addr_ptr = kv.second->addr;
        CHECK(ibv_dereg_mr(kv.second) == 0);
        mi_free(addr_ptr);
    }

    CHECK(ibv_destroy_srq(shared_rq_) == 0);
    CHECK(ibv_destroy_cq(shared_send_cq_) == 0);
    CHECK(ibv_destroy_cq(shared_recv_cq_) == 0);
    CHECK(ibv_destroy_comp_channel(send_chan_) == 0);
    CHECK(ibv_destroy_comp_channel(recv_chan_) == 0);

    CHECK(rdma_destroy_id(cm_id_) == 0);
    rdma_destroy_event_channel(ent_chan_);
}

} // namespace lightrpc