#include "tool.h"

#include <hdfspp/hdfspp.h>
#include <sys/time.h>

#include <chrono>
#include <cstring>
#include <iostream>

namespace orctool {

namespace {
uint64_t g_batch_size = 1024;
std::string g_timezone_name = "Asia/Shanghai";
}  // namespace

ORCTool::ORCTool() {}

bool ORCTool::Init(const std::string& input_path,
                   const std::string& output_path,
                   const std::vector<std::string>& cols,
                   uint64_t max_bucket_num) {
  if (!SetInputPath(input_path)) {
    return false;
  }
  if (!SetOutputPath(output_path)) {
    return false;
  }
  max_bucket_num_ = max_bucket_num;
  return Init();
}

bool ORCTool::SetInputPath(const std::string& input_path) {
  // check exist
  // check file or dir
  input_path_ = std::move(input_path);
  return true;
}

bool ORCTool::SetOutputPath(const std::string& output_path) {
  if (utils::IsHdfsPath(output_path)) {
    std::cerr << "orctool output hdfs path is not supported" << std::endl;
    return false;
  }
  // mkdir
  output_path_ = std::move(output_path);
  return true;
}

void ORCTool::SetMaxBucketNum(uint64_t max_bucket_num) {
  max_bucket_num_ = max_bucket_num;
}

bool ORCTool::Init() { return true; }

bool ORCTool::Start() {
  auto start = std::chrono::high_resolution_clock::now();
  bool res = false;
  try {
    res = _Start();
  } catch (std::exception& ex) {
    std::cerr << "Caught exception: " << ex.what() << "\n";
    return false;
  }
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<double> elapsed = end - start;
  std::cout << utils::GetDateTime()
            << " Total elasped time: " << elapsed.count() << "s." << std::endl;
  return res;
}

bool ORCTool::_Start() {
  orc::ReaderOptions options;
  ORC_UNIQUE_PTR<orc::Reader> reader =
      orc::createReader(orc::readFile(input_path_), options);

  orc::WriterOptions writer_options;
  writer_options.setFileVersion(reader->getFormatVersion());
  writer_options.setStripeSize(reader->getNumberOfStripes());
  writer_options.setCompressionBlockSize(reader->getCompressionSize());
  writer_options.setCompression(reader->getCompression());
  std::string tz = utils::GetEnv("TZ", g_timezone_name);
  writer_options.setTimezoneName(tz);

  const orc::Type& schema = reader->getType();

  orc::RowReaderOptions row_reader_options;
  ORC_UNIQUE_PTR<orc::RowReader> row_reader =
      reader->createRowReader(row_reader_options);
  std::cout << "Input file: " << input_path_ << "\nSchema: \n"
            << schema.toString() << std::endl;
  uint64_t batch_size = utils::GetEnv<int>("ORCTOOL_BATCH_SIZE", g_batch_size);
  ORC_UNIQUE_PTR<orc::ColumnVectorBatch> reader_batch =
      row_reader->createRowBatch(batch_size);

  ORC_UNIQUE_PTR<orc::OutputStream> outStream =
      orc::writeLocalFile(output_path_);
  ORC_UNIQUE_PTR<orc::Writer> writer =
      orc::createWriter(schema, outStream.get(), writer_options);

  ORC_UNIQUE_PTR<orc::ColumnVectorBatch> writer_batch =
      writer->createRowBatch(batch_size);

  auto column_copier =
      createColumnCopier(writer_batch.get(), &schema, buffer_list_);
  while (row_reader->next(*reader_batch)) {
    column_copier->Reset(reader_batch.get());
    for (uint64_t i = 0; i < reader_batch->numElements; ++i) {
      column_copier->Copy(i);
    }
    // writer->add(*reader_batch);
    writer->add(*writer_batch);
  }
  writer->close();
  std::cout << "Output file: " << output_path_ << std::endl;
  return true;
}

ColumnCopier::ColumnCopier(orc::ColumnVectorBatch* writer_batch)
    : writer_batch_(writer_batch) {}

ColumnCopier::~ColumnCopier() {}

void ColumnCopier::Reset(orc::ColumnVectorBatch* reader_batch) {
  has_nulls_ = reader_batch->hasNulls;
  num_elements_ = reader_batch->numElements;
  if (has_nulls_) {
    not_null_ = reader_batch->notNull.data();
    not_null_size_ = reader_batch->notNull.size();
  } else {
    not_null_ = nullptr;
    not_null_size_ = 0;
  }
}

void ColumnCopier::CopyBase() {
  writer_batch_->hasNulls = has_nulls_;
  writer_batch_->numElements = num_elements_;
  std::memcpy(writer_batch_->notNull.data(), not_null_, not_null_size_);
}

class VoidColumnCopier : public ColumnCopier {
 public:
  VoidColumnCopier(orc::ColumnVectorBatch* writer_batch)
      : ColumnCopier(writer_batch) {}
  ~VoidColumnCopier() override {}
  void Reset(orc::ColumnVectorBatch*) override {}
  void Copy(uint64_t) override {}
};

class LongColumnCopier : public ColumnCopier {
 private:
  const int64_t* data_;

 public:
  LongColumnCopier(orc::ColumnVectorBatch* writer_batch)
      : ColumnCopier(writer_batch) {}
  ~LongColumnCopier() override {}
  void Reset(orc::ColumnVectorBatch* reader_batch) override {
    ColumnCopier::Reset(reader_batch);
    data_ =
        dynamic_cast<const orc::LongVectorBatch*>(reader_batch)->data.data();
  }
  void Copy(uint64_t row_index) override {
    ColumnCopier::CopyBase();
    orc::LongVectorBatch* writer_batch =
        dynamic_cast<orc::LongVectorBatch*>(writer_batch_);
    writer_batch->data[row_index] = data_[row_index];
  }
};

class DoubleColumnCopier : public ColumnCopier {
 private:
  const double* data_;

 public:
  DoubleColumnCopier(orc::ColumnVectorBatch* writer_batch)
      : ColumnCopier(writer_batch) {}
  ~DoubleColumnCopier() override {}
  void Reset(orc::ColumnVectorBatch* reader_batch) override {
    ColumnCopier::Reset(reader_batch);
    data_ =
        dynamic_cast<const orc::DoubleVectorBatch*>(reader_batch)->data.data();
  }
  void Copy(uint64_t row_index) override {
    ColumnCopier::CopyBase();
    orc::DoubleVectorBatch* writer_batch =
        dynamic_cast<orc::DoubleVectorBatch*>(writer_batch_);
    writer_batch->data[row_index] = data_[row_index];
  }
};

class StringColumnCopier : public ColumnCopier {
 private:
  BufferListT& buffer_list_;
  const char* const* start_;
  const int64_t* length_;

 public:
  StringColumnCopier(orc::ColumnVectorBatch* writer_batch,
                     BufferListT& buffer_list)
      : ColumnCopier(writer_batch), buffer_list_(buffer_list) {}
  ~StringColumnCopier() override {}
  void Reset(orc::ColumnVectorBatch* reader_batch) override {
    ColumnCopier::Reset(reader_batch);
    const orc::StringVectorBatch* batch =
        dynamic_cast<const orc::StringVectorBatch*>(reader_batch);
    start_ = batch->data.data();
    length_ = batch->length.data();
  }
  void Copy(uint64_t row_index) override {
    ColumnCopier::CopyBase();
    orc::StringVectorBatch* writer_batch =
        dynamic_cast<orc::StringVectorBatch*>(writer_batch_);
    writer_batch->length[row_index] = length_[row_index];
    buffer_list_.emplace_back(*orc::getDefaultPool(), length_[row_index] + 1);
    orc::DataBuffer<char>& buffer = buffer_list_.back();
    std::memcpy(buffer.data(), start_[row_index], length_[row_index]);
    writer_batch->data[row_index] = buffer.data();
  }
};

class TimestampColumnCopier : public ColumnCopier {
 private:
  const int64_t* seconds_;
  const int64_t* nanoseconds_;

 public:
  TimestampColumnCopier(orc::ColumnVectorBatch* writer_batch)
      : ColumnCopier(writer_batch) {}
  ~TimestampColumnCopier() override {}
  void Reset(orc::ColumnVectorBatch* reader_batch) override {
    ColumnCopier::Reset(reader_batch);
    const orc::TimestampVectorBatch* batch =
        dynamic_cast<const orc::TimestampVectorBatch*>(reader_batch);
    seconds_ = batch->data.data();
    nanoseconds_ = batch->nanoseconds.data();
  }
  void Copy(uint64_t row_index) override {
    ColumnCopier::CopyBase();
    orc::TimestampVectorBatch* writer_batch =
        dynamic_cast<orc::TimestampVectorBatch*>(writer_batch);
    writer_batch->data[row_index] = seconds_[row_index];
    writer_batch->nanoseconds[row_index] = nanoseconds_[row_index];
  }
};

class Decimal128ColumnCopier : public ColumnCopier {
 private:
  const orc::Int128* data_;
  int32_t precision_;
  int32_t scale_;

 public:
  Decimal128ColumnCopier(orc::ColumnVectorBatch* writer_batch)
      : ColumnCopier(writer_batch) {}
  ~Decimal128ColumnCopier() override {}
  void Reset(orc::ColumnVectorBatch* reader_batch) override {
    ColumnCopier::Reset(reader_batch);
    const orc::Decimal128VectorBatch* batch =
        dynamic_cast<const orc::Decimal128VectorBatch*>(reader_batch);
    data_ = batch->values.data();
    precision_ = batch->precision;
    scale_ = batch->scale;
  }
  void Copy(uint64_t row_index) override {
    ColumnCopier::CopyBase();
    orc::Decimal128VectorBatch* writer_batch =
        dynamic_cast<orc::Decimal128VectorBatch*>(writer_batch_);
    writer_batch->precision = precision_;
    writer_batch->scale = scale_;
    writer_batch->values[row_index] = data_[row_index];
  }
};

class Decimal64ColumnCopier : public ColumnCopier {
 private:
  const int64_t* data_;
  int32_t precision_;
  int32_t scale_;

 public:
  Decimal64ColumnCopier(orc::ColumnVectorBatch* writer_batch)
      : ColumnCopier(writer_batch) {}
  ~Decimal64ColumnCopier() override {}
  void Reset(orc::ColumnVectorBatch* reader_batch) override {
    ColumnCopier::Reset(reader_batch);
    const orc::Decimal64VectorBatch* batch =
        dynamic_cast<const orc::Decimal64VectorBatch*>(reader_batch);
    data_ = batch->values.data();
    precision_ = batch->precision;
    scale_ = batch->scale;
  }
  void Copy(uint64_t row_index) override {
    ColumnCopier::CopyBase();
    orc::Decimal64VectorBatch* writer_batch =
        dynamic_cast<orc::Decimal64VectorBatch*>(writer_batch_);
    writer_batch->precision = precision_;
    writer_batch->scale = scale_;
    writer_batch->values[row_index] = data_[row_index];
  }
};

class ListColumnCopier : public ColumnCopier {
 private:
  const int64_t* offsets_;
  ORC_UNIQUE_PTR<ColumnCopier> element_copier_;

 public:
  ListColumnCopier(orc::ColumnVectorBatch* writer_batch, const orc::Type* type,
                   BufferListT& buffer_list)
      : ColumnCopier(writer_batch) {
    orc::ListVectorBatch* batch =
        dynamic_cast<orc::ListVectorBatch*>(writer_batch);
    element_copier_ = createColumnCopier(batch->elements.get(),
                                         type->getSubtype(0), buffer_list);
  }
  ~ListColumnCopier() override {}
  void Reset(orc::ColumnVectorBatch* reader_batch) override {
    ColumnCopier::Reset(reader_batch);
    const orc::ListVectorBatch* batch =
        dynamic_cast<const orc::ListVectorBatch*>(reader_batch);
    offsets_ = batch->offsets.data();
    element_copier_->Reset(batch->elements.get());
  }
  void Copy(uint64_t row_index) override {
    ColumnCopier::CopyBase();
    orc::ListVectorBatch* writer_batch =
        dynamic_cast<orc::ListVectorBatch*>(writer_batch_);
    writer_batch->offsets[row_index] = offsets_[row_index];
    writer_batch->offsets[row_index + 1] = offsets_[row_index + 1];
    for (int64_t o = offsets_[row_index]; o < offsets_[row_index + 1]; ++o) {
      element_copier_->Copy(static_cast<uint64_t>(o));
    }
  }  // copy
};

class MapColumnCopier : public ColumnCopier {
 private:
  const int64_t* offsets_;
  ORC_UNIQUE_PTR<ColumnCopier> key_copier_;
  ORC_UNIQUE_PTR<ColumnCopier> element_copier_;

 public:
  MapColumnCopier(orc::ColumnVectorBatch* writer_batch, const orc::Type* type,
                  BufferListT& buffer_list)
      : ColumnCopier(writer_batch) {
    orc::MapVectorBatch* batch =
        dynamic_cast<orc::MapVectorBatch*>(writer_batch);
    key_copier_ =
        createColumnCopier(batch->keys.get(), type->getSubtype(0), buffer_list);
    element_copier_ = createColumnCopier(batch->elements.get(),
                                         type->getSubtype(1), buffer_list);
  }
  ~MapColumnCopier() override {}
  void Reset(orc::ColumnVectorBatch* reader_batch) override {
    ColumnCopier::Reset(reader_batch);
    const orc::MapVectorBatch* batch =
        dynamic_cast<const orc::MapVectorBatch*>(reader_batch);
    offsets_ = batch->offsets.data();
    key_copier_->Reset(batch->keys.get());
    element_copier_->Reset(batch->elements.get());
  }
  void Copy(uint64_t row_index) override {
    ColumnCopier::CopyBase();
    orc::MapVectorBatch* writer_batch =
        dynamic_cast<orc::MapVectorBatch*>(writer_batch_);
    writer_batch->offsets[row_index] = offsets_[row_index];
    writer_batch->offsets[row_index + 1] = offsets_[row_index + 1];
    for (uint64_t o = offsets_[row_index]; o < offsets_[row_index + 1]; ++o) {
      auto oo = static_cast<uint64_t>(o);
      key_copier_->Copy(oo);
      element_copier_->Copy(oo);
    }
  }  // copy
};

class StructColumnCopier : public ColumnCopier {
 private:
  std::vector<ORC_UNIQUE_PTR<ColumnCopier>> field_copier_;

 public:
  StructColumnCopier(orc::ColumnVectorBatch* writer_batch,
                     const orc::Type* type, BufferListT& buffer_list)
      : ColumnCopier(writer_batch) {
    orc::StructVectorBatch* batch =
        dynamic_cast<orc::StructVectorBatch*>(writer_batch);
    for (uint64_t i = 0; i < type->getSubtypeCount(); ++i) {
      field_copier_.emplace_back(createColumnCopier(
          batch->fields[i], type->getSubtype(i), buffer_list));
    }
  }
  ~StructColumnCopier() override {}
  void Reset(orc::ColumnVectorBatch* reader_batch) override {
    ColumnCopier::Reset(reader_batch);
    const orc::StructVectorBatch* batch =
        dynamic_cast<const orc::StructVectorBatch*>(reader_batch);
    for (size_t i = 0; i < field_copier_.size(); ++i) {
      field_copier_[i]->Reset(batch->fields[i]);
    }
  }
  void Copy(uint64_t row_index) override {
    ColumnCopier::CopyBase();
    for (size_t i = 0; i < field_copier_.size(); ++i) {
      field_copier_[i]->Copy(row_index);
    }
  }  // copy
};

class UnionColumnCopier : public ColumnCopier {
 private:
  const unsigned char* tags_;
  const uint64_t* offsets_;
  std::vector<ORC_UNIQUE_PTR<ColumnCopier>> field_copier_;

 public:
  UnionColumnCopier(orc::ColumnVectorBatch* writer_batch, const orc::Type* type,
                    BufferListT& buffer_list)
      : ColumnCopier(writer_batch) {
    orc::UnionVectorBatch* batch =
        dynamic_cast<orc::UnionVectorBatch*>(writer_batch);
    for (uint64_t i = 0; i < type->getSubtypeCount(); ++i) {
      field_copier_.emplace_back(createColumnCopier(
          batch->children[i], type->getSubtype(i), buffer_list));
    }
  }
  ~UnionColumnCopier() override {}
  void Reset(orc::ColumnVectorBatch* reader_batch) override {
    ColumnCopier::Reset(reader_batch);
    const orc::UnionVectorBatch* batch =
        dynamic_cast<const orc::UnionVectorBatch*>(reader_batch);
    tags_ = batch->tags.data();
    offsets_ = batch->offsets.data();
    for (size_t i = 0; i < field_copier_.size(); ++i) {
      field_copier_[i]->Reset(batch->children[i]);
    }
  }
  void Copy(uint64_t row_index) override {
    ColumnCopier::CopyBase();
    orc::UnionVectorBatch* writer_batch =
        dynamic_cast<orc::UnionVectorBatch*>(writer_batch_);
    writer_batch->tags[row_index] = tags_[row_index];
    writer_batch->offsets[row_index] = offsets_[row_index];
    field_copier_[tags_[row_index]]->Copy(offsets_[row_index]);
  }  // copy
};

ORC_UNIQUE_PTR<ColumnCopier> createColumnCopier(
    orc::ColumnVectorBatch* writer_batch, const orc::Type* type,
    BufferListT& buffer_list) {
  ColumnCopier* result = nullptr;
  if (type == nullptr) {
    result = new VoidColumnCopier(writer_batch);
    return std::unique_ptr<ColumnCopier>(result);
  }
  auto tp = type->getKind();
  switch (tp) {
    case orc::BOOLEAN:
    case orc::BYTE:
    case orc::INT:
    case orc::SHORT:
    case orc::LONG:
    case orc::DATE:
      result = new LongColumnCopier(writer_batch);
      break;
    case orc::FLOAT:
    case orc::DOUBLE:
      result = new DoubleColumnCopier(writer_batch);
      break;
    case orc::STRING:
    case orc::CHAR:
    case orc::VARCHAR:
    case orc::BINARY:
      result = new StringColumnCopier(writer_batch, buffer_list);
      break;
    case orc::TIMESTAMP:
    case orc::TIMESTAMP_INSTANT:
      result = new TimestampColumnCopier(writer_batch);
      break;
    case orc::LIST:
      result = new ListColumnCopier(writer_batch, type, buffer_list);
      break;
    case orc::MAP:
      result = new MapColumnCopier(writer_batch, type, buffer_list);
      break;
    case orc::STRUCT:
      result = new StructColumnCopier(writer_batch, type, buffer_list);
      break;
    case orc::UNION:
      result = new UnionColumnCopier(writer_batch, type, buffer_list);
      break;
    case orc::DECIMAL:
      if (type->getPrecision() == 0 || type->getPrecision() > 18) {
        result = new Decimal128ColumnCopier(writer_batch);
      } else {
        result = new Decimal64ColumnCopier(writer_batch);
      }
      break;
    default:
      std::cerr << "unknown batch type: " << tp << std::endl;
      result = new VoidColumnCopier(writer_batch);
  }  // switch
  return std::unique_ptr<ColumnCopier>(result);
}

void ORCTool::_StartConcurrent() {
  utils::TaskThreadPool thread_pool;
  std::atomic<bool> success(true);
  for (int i = 0; i < 10; ++i) {
    thread_pool.AddTask([this, &success]() {
      if (success && !_Start()) {
        success = false;
      }
    });
  }

  int thread_num = utils::GetEnv<int>("ORCTOOL_THREAD_NUM", 8);
  thread_pool.Start(thread_num);
  thread_pool.ShutDown();
}

namespace utils {

bool IsHdfsPath(const std::string& path) {
  return std::strncmp(path.c_str(), "hdfs://", 7) == 0;
}

template <>
int GetEnv<int>(const char* key, int default_value) {
  const char* value = std::getenv(key);
  if (value == nullptr) {
    return default_value;
  }
  try {
    return std::stoi(value);
  } catch (...) {
    return default_value;
  }
}

const char* GetDateTime(void) {
  static char buf[200];
  std::time_t t = std::time(nullptr);
  std::strftime(buf, sizeof(buf), "[%Y-%m-%d %H:%M:%S]", std::localtime(&t));
  return buf;
}

}  // namespace utils
}  // namespace orctool
