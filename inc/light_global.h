#pragma once

#include <rdma/rdma_cma.h>

#include <boost/asio.hpp>
#include <boost/lockfree/queue.hpp>
#include <unordered_map>

namespace lightrpc {

struct RemoteInfo {
  uint32_t remote_key;
  uint64_t remote_addr;
  RemoteInfo(const RemoteInfo &) = default;
};

// If the local_port is set to a negative number, the system will automatically
// allocate a port.
struct ResourceConfig {
  std::string local_ip;
  int local_port;
  int block_pool_size;
  int num_threads;
  ResourceConfig(const ResourceConfig &) = default;
};

struct QueuePairContext {
  // The hashmap from rpc id to large message mr.
  std::mutex mr_mtx_;
  std::unordered_map<uint32_t, ibv_mr *> mr_map_;
  // The hashmap from rpc id to remote info.
  std::mutex rinfo_mtx_;
  std::unordered_map<uint32_t, std::promise<RemoteInfo> &> rinfo_map_;
  ~QueuePairContext() = default;
};

class ThreadInfo {
 public:
  ThreadInfo(int idx) : vec_idx(idx), total_cnt(0), complete_cnt(0) {}
  uint64_t GetRemainTaskNum() const { return total_cnt - complete_cnt; }
  int GetVecIndex() const { return vec_idx; }
  void IncTotalCount() { total_cnt++; }
  void IncCompleteCount() { complete_cnt++; }

 private:
  int vec_idx;
  std::atomic<uint64_t> total_cnt;
  std::atomic<uint64_t> complete_cnt;
};

/* Some help functions */
// Query key attributes of the bound device.
void QueryDeviceAttribute(ibv_context *ctx);
// Register memory of specified size and protection domain.
ibv_mr *AllocMemoryRegion(uint32_t msg_len, ibv_pd *pd);
// Send the message in the block and set imm_data to msg_len. This func sets wr_id to block addr.
// User can return the block by polling send work completion.
void SendSmallMessage(ibv_qp *qp, uint64_t block_addr, uint32_t msg_len, uint32_t lkey);
// Send control messages through inline mode. For notify message, this func sets imm_data to message
// length. For authority message, this func sets imm_data to 0.
void SendControlMessage(ibv_qp *qp, const char *send_addr, uint32_t send_len, uint32_t imm_data=0);
// Write the large message (in msg_mr) to remote. This func sets imm_data to rpc_id.
void WriteLargeMessage(ibv_qp *qp, ibv_mr *msg_mr, uint32_t rpc_id, RemoteInfo &target);

class GlobalResource {
 public:
  GlobalResource(const ResourceConfig &config);
  virtual ~GlobalResource();

  void CreateControlID();
  void BindLocalDevice();
  void CreateBlockPool();
  void CreateSharedQueue();
  void StartWorkerThread();
  void StartPollerThread();

  void ObtainOneBlock(uint64_t &addr);
  void ReturnOneBlock(uint64_t addr);

  // Create a qp with the shared queues.
  void CreateQueuePair(rdma_cm_id *conn_id);
  // Post a recv request to the srq.
  void PostOneRecvRequest(uint64_t recv_addr);
  // Use a single thread to poll wc from the recv cq.
  void PollWorkCompletion();

  void ProcessSendWorkCompletion(ibv_wc &wc);
  virtual void ProcessRecvWorkCompletion(ibv_wc &wc) = 0;

  int SelectTargetThread();
  void GetAndPostOneBlock();

  void GenericSendFunc(uint32_t msg_len,
                       char *&msg_buf,
                       const std::function<void()> &serialize_func,
                       uint32_t rpc_id,
                       ibv_qp *conn_qp);
  void ProcessNotifyMessage(uint32_t imm_data, uint64_t recv_addr, ibv_qp *conn_qp);
  void ProcessAuthorityMessage(uint64_t recv_addr, ibv_qp *conn_qp);
  ibv_mr *GetAndEraseMemoryRegion(uint32_t imm_data, ibv_qp *conn_qp);

 protected:
  ResourceConfig config_;
  rdma_cm_id *cm_id_;

  // Shared rdma queue resources.
  ibv_srq *shared_rq_;
  ibv_cq *shared_send_cq_;
  ibv_cq *shared_recv_cq_;

  // Block pool.
  ibv_mr *shared_mr_;
  int num_blocks_;
  boost::lockfree::queue<uint64_t> addr_queue_;

  // Thread pool.
  std::vector<std::thread> thread_pool_;
  std::unordered_map<std::thread::id, ThreadInfo *> thread_info_map_;
  std::vector<boost::asio::io_context *> service_pool_;
  std::vector<boost::asio::io_context::work *> work_pool_;

  // Poller (recv/send wc) and send wc handler.
  std::thread wc_poller_;
  std::atomic<bool> poller_stop_;
  std::thread wc_handler_;
  boost::asio::io_context handler_service_;
  boost::asio::io_context::work handler_work_;
};

}  // namespace lightrpc