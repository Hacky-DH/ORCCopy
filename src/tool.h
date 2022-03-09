#pragma once

#include <atomic>
#include <cstring>
#include <memory>
#include <mutex>
#include <orc/OrcFile.hh>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace orctool {

using BufferListT = std::list<orc::DataBuffer<char>>;

class ORCTool {
 public:
  ORCTool();
  bool Init();
  bool Init(const std::string& input_path, const std::string& output_path,
            const std::vector<std::string>& cols,
            uint64_t max_bucket_num = 1024);
  bool Start();

  bool SetInputPath(const std::string& input_path);
  bool SetOutputPath(const std::string& output_path);
  void SetMaxBucketNum(uint64_t);

 private:
  bool _Start();
  void _StartConcurrent();

 private:
  uint64_t max_bucket_num_;
  std::string input_path_;
  std::string output_path_;
  BufferListT buffer_list_;
};

class ColumnCopier {
 protected:
  orc::ColumnVectorBatch* writer_batch_;
  bool has_nulls_;
  const char* not_null_;
  uint64_t not_null_size_;
  uint64_t num_elements_;

  void CopyBase();

 public:
  ColumnCopier(orc::ColumnVectorBatch*);
  virtual ~ColumnCopier();
  virtual void Copy(uint64_t) = 0;
  // should be called once at the start of each batch of rows
  virtual void Reset(orc::ColumnVectorBatch*);
};

ORC_UNIQUE_PTR<ColumnCopier> createColumnCopier(orc::ColumnVectorBatch*,
                                                const orc::Type*, BufferListT&);

namespace utils {

bool IsHdfsPath(const std::string& path);

template <typename T = std::string>
T GetEnv(const char* key, T default_value) {
  const char* value = std::getenv(key);
  if (value == nullptr) {
    return default_value;
  }
  return T(value);
}

template <>
int GetEnv<int>(const char* key, int default_value);

typedef std::function<void()> TaskFunc;

class TaskThread {
 public:
  TaskThread(TaskFunc fn) : task_thread_(fn) {}

  ~TaskThread() {
    if (Joinable()) {
      Join();
    }
  }

  bool Joinable() { return task_thread_.joinable(); }

  void Join() {
    if (Joinable()) {
      task_thread_.join();
    }
  }

 private:
  std::thread task_thread_;
};

class TaskQueue {
 public:
  TaskQueue() {}
  TaskFunc Pop() {
    std::unique_lock<std::mutex> lock(mu_);
    auto element = tasks_.front();
    tasks_.pop();
    return element;
  }

  void Push(const TaskFunc& element) {
    std::unique_lock<std::mutex> lock(mu_);
    tasks_.push(element);
  }
  bool IsEmpty() {
    std::unique_lock<std::mutex> lock(mu_);
    return tasks_.empty();
  }
  void Clear() {
    std::unique_lock<std::mutex> lock(mu_);
    while (!tasks_.empty()) tasks_.pop();
  }

 private:
  std::mutex mu_;
  std::queue<TaskFunc> tasks_;
};

class TaskThreadPool {
 public:
  TaskThreadPool() {}

  ~TaskThreadPool() {}

  void AddTask(TaskFunc fn) { task_queues_.Push(fn); }
  void Start(size_t num_threads) {
    size_t max_thread_num = std::thread::hardware_concurrency();
    if (num_threads > max_thread_num) num_threads = max_thread_num;
    for (size_t i = 0; i < num_threads; ++i) {
      task_threads_.emplace_back(new TaskThread([this, i]() { Loop(i); }));
    }
  }

  void Wait() {
    for (auto& task_thread : task_threads_) {
      task_thread->Join();
    }
  }
  void ShutDown() {
    Wait();
    task_threads_.clear();
    task_queues_.Clear();
  }

 private:
  void Loop(size_t i) {
    while (!task_queues_.IsEmpty()) {
      TaskFunc fn = task_queues_.Pop();
      fn();
    }
  }

 private:
  std::vector<std::unique_ptr<TaskThread>> task_threads_;
  TaskQueue task_queues_;
};

const char* GetDateTime(void);

}  // namespace utils
}  // namespace orctool
