// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: BitData.proto

#ifndef PROTOBUF_BitData_2eproto__INCLUDED
#define PROTOBUF_BitData_2eproto__INCLUDED

#include <string>

#include <google/protobuf/stubs/common.h>

#if GOOGLE_PROTOBUF_VERSION < 2005000
#error This file was generated by a newer version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please update
#error your headers.
#endif
#if 2005000 < GOOGLE_PROTOBUF_MIN_PROTOC_VERSION
#error This file was generated by an older version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please
#error regenerate this file with a newer version of protoc.
#endif

#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/generated_enum_reflection.h>
#include <google/protobuf/unknown_field_set.h>
#include "ExecutionProtos.pb.h"
#include "Coordination.pb.h"
#include "UserBitShared.pb.h"
// @@protoc_insertion_point(includes)

namespace exec {
namespace bit {
namespace data {

// Internal implementation detail -- do not call these.
void  protobuf_AddDesc_BitData_2eproto();
void protobuf_AssignDesc_BitData_2eproto();
void protobuf_ShutdownFile_BitData_2eproto();

class BitClientHandshake;
class BitServerHandshake;
class FragmentRecordBatch;

enum RpcType {
  HANDSHAKE = 0,
  ACK = 1,
  GOODBYE = 2,
  REQ_RECORD_BATCH = 3
};
bool RpcType_IsValid(int value);
const RpcType RpcType_MIN = HANDSHAKE;
const RpcType RpcType_MAX = REQ_RECORD_BATCH;
const int RpcType_ARRAYSIZE = RpcType_MAX + 1;

const ::google::protobuf::EnumDescriptor* RpcType_descriptor();
inline const ::std::string& RpcType_Name(RpcType value) {
  return ::google::protobuf::internal::NameOfEnum(
    RpcType_descriptor(), value);
}
inline bool RpcType_Parse(
    const ::std::string& name, RpcType* value) {
  return ::google::protobuf::internal::ParseNamedEnum<RpcType>(
    RpcType_descriptor(), name, value);
}
// ===================================================================

class BitClientHandshake : public ::google::protobuf::Message {
 public:
  BitClientHandshake();
  virtual ~BitClientHandshake();

  BitClientHandshake(const BitClientHandshake& from);

  inline BitClientHandshake& operator=(const BitClientHandshake& from) {
    CopyFrom(from);
    return *this;
  }

  inline const ::google::protobuf::UnknownFieldSet& unknown_fields() const {
    return _unknown_fields_;
  }

  inline ::google::protobuf::UnknownFieldSet* mutable_unknown_fields() {
    return &_unknown_fields_;
  }

  static const ::google::protobuf::Descriptor* descriptor();
  static const BitClientHandshake& default_instance();

  void Swap(BitClientHandshake* other);

  // implements Message ----------------------------------------------

  BitClientHandshake* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const BitClientHandshake& from);
  void MergeFrom(const BitClientHandshake& from);
  void Clear();
  bool IsInitialized() const;

  int ByteSize() const;
  bool MergePartialFromCodedStream(
      ::google::protobuf::io::CodedInputStream* input);
  void SerializeWithCachedSizes(
      ::google::protobuf::io::CodedOutputStream* output) const;
  ::google::protobuf::uint8* SerializeWithCachedSizesToArray(::google::protobuf::uint8* output) const;
  int GetCachedSize() const { return _cached_size_; }
  private:
  void SharedCtor();
  void SharedDtor();
  void SetCachedSize(int size) const;
  public:

  ::google::protobuf::Metadata GetMetadata() const;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  // optional int32 rpc_version = 1;
  inline bool has_rpc_version() const;
  inline void clear_rpc_version();
  static const int kRpcVersionFieldNumber = 1;
  inline ::google::protobuf::int32 rpc_version() const;
  inline void set_rpc_version(::google::protobuf::int32 value);

  // optional .exec.shared.RpcChannel channel = 2 [default = BIT_DATA];
  inline bool has_channel() const;
  inline void clear_channel();
  static const int kChannelFieldNumber = 2;
  inline ::exec::shared::RpcChannel channel() const;
  inline void set_channel(::exec::shared::RpcChannel value);

  // @@protoc_insertion_point(class_scope:exec.bit.data.BitClientHandshake)
 private:
  inline void set_has_rpc_version();
  inline void clear_has_rpc_version();
  inline void set_has_channel();
  inline void clear_has_channel();

  ::google::protobuf::UnknownFieldSet _unknown_fields_;

  ::google::protobuf::int32 rpc_version_;
  int channel_;

  mutable int _cached_size_;
  ::google::protobuf::uint32 _has_bits_[(2 + 31) / 32];

  friend void  protobuf_AddDesc_BitData_2eproto();
  friend void protobuf_AssignDesc_BitData_2eproto();
  friend void protobuf_ShutdownFile_BitData_2eproto();

  void InitAsDefaultInstance();
  static BitClientHandshake* default_instance_;
};
// -------------------------------------------------------------------

class BitServerHandshake : public ::google::protobuf::Message {
 public:
  BitServerHandshake();
  virtual ~BitServerHandshake();

  BitServerHandshake(const BitServerHandshake& from);

  inline BitServerHandshake& operator=(const BitServerHandshake& from) {
    CopyFrom(from);
    return *this;
  }

  inline const ::google::protobuf::UnknownFieldSet& unknown_fields() const {
    return _unknown_fields_;
  }

  inline ::google::protobuf::UnknownFieldSet* mutable_unknown_fields() {
    return &_unknown_fields_;
  }

  static const ::google::protobuf::Descriptor* descriptor();
  static const BitServerHandshake& default_instance();

  void Swap(BitServerHandshake* other);

  // implements Message ----------------------------------------------

  BitServerHandshake* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const BitServerHandshake& from);
  void MergeFrom(const BitServerHandshake& from);
  void Clear();
  bool IsInitialized() const;

  int ByteSize() const;
  bool MergePartialFromCodedStream(
      ::google::protobuf::io::CodedInputStream* input);
  void SerializeWithCachedSizes(
      ::google::protobuf::io::CodedOutputStream* output) const;
  ::google::protobuf::uint8* SerializeWithCachedSizesToArray(::google::protobuf::uint8* output) const;
  int GetCachedSize() const { return _cached_size_; }
  private:
  void SharedCtor();
  void SharedDtor();
  void SetCachedSize(int size) const;
  public:

  ::google::protobuf::Metadata GetMetadata() const;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  // optional int32 rpc_version = 1;
  inline bool has_rpc_version() const;
  inline void clear_rpc_version();
  static const int kRpcVersionFieldNumber = 1;
  inline ::google::protobuf::int32 rpc_version() const;
  inline void set_rpc_version(::google::protobuf::int32 value);

  // @@protoc_insertion_point(class_scope:exec.bit.data.BitServerHandshake)
 private:
  inline void set_has_rpc_version();
  inline void clear_has_rpc_version();

  ::google::protobuf::UnknownFieldSet _unknown_fields_;

  ::google::protobuf::int32 rpc_version_;

  mutable int _cached_size_;
  ::google::protobuf::uint32 _has_bits_[(1 + 31) / 32];

  friend void  protobuf_AddDesc_BitData_2eproto();
  friend void protobuf_AssignDesc_BitData_2eproto();
  friend void protobuf_ShutdownFile_BitData_2eproto();

  void InitAsDefaultInstance();
  static BitServerHandshake* default_instance_;
};
// -------------------------------------------------------------------

class FragmentRecordBatch : public ::google::protobuf::Message {
 public:
  FragmentRecordBatch();
  virtual ~FragmentRecordBatch();

  FragmentRecordBatch(const FragmentRecordBatch& from);

  inline FragmentRecordBatch& operator=(const FragmentRecordBatch& from) {
    CopyFrom(from);
    return *this;
  }

  inline const ::google::protobuf::UnknownFieldSet& unknown_fields() const {
    return _unknown_fields_;
  }

  inline ::google::protobuf::UnknownFieldSet* mutable_unknown_fields() {
    return &_unknown_fields_;
  }

  static const ::google::protobuf::Descriptor* descriptor();
  static const FragmentRecordBatch& default_instance();

  void Swap(FragmentRecordBatch* other);

  // implements Message ----------------------------------------------

  FragmentRecordBatch* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const FragmentRecordBatch& from);
  void MergeFrom(const FragmentRecordBatch& from);
  void Clear();
  bool IsInitialized() const;

  int ByteSize() const;
  bool MergePartialFromCodedStream(
      ::google::protobuf::io::CodedInputStream* input);
  void SerializeWithCachedSizes(
      ::google::protobuf::io::CodedOutputStream* output) const;
  ::google::protobuf::uint8* SerializeWithCachedSizesToArray(::google::protobuf::uint8* output) const;
  int GetCachedSize() const { return _cached_size_; }
  private:
  void SharedCtor();
  void SharedDtor();
  void SetCachedSize(int size) const;
  public:

  ::google::protobuf::Metadata GetMetadata() const;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  // optional .exec.shared.QueryId query_id = 1;
  inline bool has_query_id() const;
  inline void clear_query_id();
  static const int kQueryIdFieldNumber = 1;
  inline const ::exec::shared::QueryId& query_id() const;
  inline ::exec::shared::QueryId* mutable_query_id();
  inline ::exec::shared::QueryId* release_query_id();
  inline void set_allocated_query_id(::exec::shared::QueryId* query_id);

  // optional int32 receiving_major_fragment_id = 2;
  inline bool has_receiving_major_fragment_id() const;
  inline void clear_receiving_major_fragment_id();
  static const int kReceivingMajorFragmentIdFieldNumber = 2;
  inline ::google::protobuf::int32 receiving_major_fragment_id() const;
  inline void set_receiving_major_fragment_id(::google::protobuf::int32 value);

  // repeated int32 receiving_minor_fragment_id = 3;
  inline int receiving_minor_fragment_id_size() const;
  inline void clear_receiving_minor_fragment_id();
  static const int kReceivingMinorFragmentIdFieldNumber = 3;
  inline ::google::protobuf::int32 receiving_minor_fragment_id(int index) const;
  inline void set_receiving_minor_fragment_id(int index, ::google::protobuf::int32 value);
  inline void add_receiving_minor_fragment_id(::google::protobuf::int32 value);
  inline const ::google::protobuf::RepeatedField< ::google::protobuf::int32 >&
      receiving_minor_fragment_id() const;
  inline ::google::protobuf::RepeatedField< ::google::protobuf::int32 >*
      mutable_receiving_minor_fragment_id();

  // optional int32 sending_major_fragment_id = 4;
  inline bool has_sending_major_fragment_id() const;
  inline void clear_sending_major_fragment_id();
  static const int kSendingMajorFragmentIdFieldNumber = 4;
  inline ::google::protobuf::int32 sending_major_fragment_id() const;
  inline void set_sending_major_fragment_id(::google::protobuf::int32 value);

  // optional int32 sending_minor_fragment_id = 5;
  inline bool has_sending_minor_fragment_id() const;
  inline void clear_sending_minor_fragment_id();
  static const int kSendingMinorFragmentIdFieldNumber = 5;
  inline ::google::protobuf::int32 sending_minor_fragment_id() const;
  inline void set_sending_minor_fragment_id(::google::protobuf::int32 value);

  // optional .exec.shared.RecordBatchDef def = 6;
  inline bool has_def() const;
  inline void clear_def();
  static const int kDefFieldNumber = 6;
  inline const ::exec::shared::RecordBatchDef& def() const;
  inline ::exec::shared::RecordBatchDef* mutable_def();
  inline ::exec::shared::RecordBatchDef* release_def();
  inline void set_allocated_def(::exec::shared::RecordBatchDef* def);

  // optional bool isLastBatch = 7;
  inline bool has_islastbatch() const;
  inline void clear_islastbatch();
  static const int kIsLastBatchFieldNumber = 7;
  inline bool islastbatch() const;
  inline void set_islastbatch(bool value);

  // @@protoc_insertion_point(class_scope:exec.bit.data.FragmentRecordBatch)
 private:
  inline void set_has_query_id();
  inline void clear_has_query_id();
  inline void set_has_receiving_major_fragment_id();
  inline void clear_has_receiving_major_fragment_id();
  inline void set_has_sending_major_fragment_id();
  inline void clear_has_sending_major_fragment_id();
  inline void set_has_sending_minor_fragment_id();
  inline void clear_has_sending_minor_fragment_id();
  inline void set_has_def();
  inline void clear_has_def();
  inline void set_has_islastbatch();
  inline void clear_has_islastbatch();

  ::google::protobuf::UnknownFieldSet _unknown_fields_;

  ::exec::shared::QueryId* query_id_;
  ::google::protobuf::RepeatedField< ::google::protobuf::int32 > receiving_minor_fragment_id_;
  ::google::protobuf::int32 receiving_major_fragment_id_;
  ::google::protobuf::int32 sending_major_fragment_id_;
  ::exec::shared::RecordBatchDef* def_;
  ::google::protobuf::int32 sending_minor_fragment_id_;
  bool islastbatch_;

  mutable int _cached_size_;
  ::google::protobuf::uint32 _has_bits_[(7 + 31) / 32];

  friend void  protobuf_AddDesc_BitData_2eproto();
  friend void protobuf_AssignDesc_BitData_2eproto();
  friend void protobuf_ShutdownFile_BitData_2eproto();

  void InitAsDefaultInstance();
  static FragmentRecordBatch* default_instance_;
};
// ===================================================================


// ===================================================================

// BitClientHandshake

// optional int32 rpc_version = 1;
inline bool BitClientHandshake::has_rpc_version() const {
  return (_has_bits_[0] & 0x00000001u) != 0;
}
inline void BitClientHandshake::set_has_rpc_version() {
  _has_bits_[0] |= 0x00000001u;
}
inline void BitClientHandshake::clear_has_rpc_version() {
  _has_bits_[0] &= ~0x00000001u;
}
inline void BitClientHandshake::clear_rpc_version() {
  rpc_version_ = 0;
  clear_has_rpc_version();
}
inline ::google::protobuf::int32 BitClientHandshake::rpc_version() const {
  return rpc_version_;
}
inline void BitClientHandshake::set_rpc_version(::google::protobuf::int32 value) {
  set_has_rpc_version();
  rpc_version_ = value;
}

// optional .exec.shared.RpcChannel channel = 2 [default = BIT_DATA];
inline bool BitClientHandshake::has_channel() const {
  return (_has_bits_[0] & 0x00000002u) != 0;
}
inline void BitClientHandshake::set_has_channel() {
  _has_bits_[0] |= 0x00000002u;
}
inline void BitClientHandshake::clear_has_channel() {
  _has_bits_[0] &= ~0x00000002u;
}
inline void BitClientHandshake::clear_channel() {
  channel_ = 1;
  clear_has_channel();
}
inline ::exec::shared::RpcChannel BitClientHandshake::channel() const {
  return static_cast< ::exec::shared::RpcChannel >(channel_);
}
inline void BitClientHandshake::set_channel(::exec::shared::RpcChannel value) {
  assert(::exec::shared::RpcChannel_IsValid(value));
  set_has_channel();
  channel_ = value;
}

// -------------------------------------------------------------------

// BitServerHandshake

// optional int32 rpc_version = 1;
inline bool BitServerHandshake::has_rpc_version() const {
  return (_has_bits_[0] & 0x00000001u) != 0;
}
inline void BitServerHandshake::set_has_rpc_version() {
  _has_bits_[0] |= 0x00000001u;
}
inline void BitServerHandshake::clear_has_rpc_version() {
  _has_bits_[0] &= ~0x00000001u;
}
inline void BitServerHandshake::clear_rpc_version() {
  rpc_version_ = 0;
  clear_has_rpc_version();
}
inline ::google::protobuf::int32 BitServerHandshake::rpc_version() const {
  return rpc_version_;
}
inline void BitServerHandshake::set_rpc_version(::google::protobuf::int32 value) {
  set_has_rpc_version();
  rpc_version_ = value;
}

// -------------------------------------------------------------------

// FragmentRecordBatch

// optional .exec.shared.QueryId query_id = 1;
inline bool FragmentRecordBatch::has_query_id() const {
  return (_has_bits_[0] & 0x00000001u) != 0;
}
inline void FragmentRecordBatch::set_has_query_id() {
  _has_bits_[0] |= 0x00000001u;
}
inline void FragmentRecordBatch::clear_has_query_id() {
  _has_bits_[0] &= ~0x00000001u;
}
inline void FragmentRecordBatch::clear_query_id() {
  if (query_id_ != NULL) query_id_->::exec::shared::QueryId::Clear();
  clear_has_query_id();
}
inline const ::exec::shared::QueryId& FragmentRecordBatch::query_id() const {
  return query_id_ != NULL ? *query_id_ : *default_instance_->query_id_;
}
inline ::exec::shared::QueryId* FragmentRecordBatch::mutable_query_id() {
  set_has_query_id();
  if (query_id_ == NULL) query_id_ = new ::exec::shared::QueryId;
  return query_id_;
}
inline ::exec::shared::QueryId* FragmentRecordBatch::release_query_id() {
  clear_has_query_id();
  ::exec::shared::QueryId* temp = query_id_;
  query_id_ = NULL;
  return temp;
}
inline void FragmentRecordBatch::set_allocated_query_id(::exec::shared::QueryId* query_id) {
  delete query_id_;
  query_id_ = query_id;
  if (query_id) {
    set_has_query_id();
  } else {
    clear_has_query_id();
  }
}

// optional int32 receiving_major_fragment_id = 2;
inline bool FragmentRecordBatch::has_receiving_major_fragment_id() const {
  return (_has_bits_[0] & 0x00000002u) != 0;
}
inline void FragmentRecordBatch::set_has_receiving_major_fragment_id() {
  _has_bits_[0] |= 0x00000002u;
}
inline void FragmentRecordBatch::clear_has_receiving_major_fragment_id() {
  _has_bits_[0] &= ~0x00000002u;
}
inline void FragmentRecordBatch::clear_receiving_major_fragment_id() {
  receiving_major_fragment_id_ = 0;
  clear_has_receiving_major_fragment_id();
}
inline ::google::protobuf::int32 FragmentRecordBatch::receiving_major_fragment_id() const {
  return receiving_major_fragment_id_;
}
inline void FragmentRecordBatch::set_receiving_major_fragment_id(::google::protobuf::int32 value) {
  set_has_receiving_major_fragment_id();
  receiving_major_fragment_id_ = value;
}

// repeated int32 receiving_minor_fragment_id = 3;
inline int FragmentRecordBatch::receiving_minor_fragment_id_size() const {
  return receiving_minor_fragment_id_.size();
}
inline void FragmentRecordBatch::clear_receiving_minor_fragment_id() {
  receiving_minor_fragment_id_.Clear();
}
inline ::google::protobuf::int32 FragmentRecordBatch::receiving_minor_fragment_id(int index) const {
  return receiving_minor_fragment_id_.Get(index);
}
inline void FragmentRecordBatch::set_receiving_minor_fragment_id(int index, ::google::protobuf::int32 value) {
  receiving_minor_fragment_id_.Set(index, value);
}
inline void FragmentRecordBatch::add_receiving_minor_fragment_id(::google::protobuf::int32 value) {
  receiving_minor_fragment_id_.Add(value);
}
inline const ::google::protobuf::RepeatedField< ::google::protobuf::int32 >&
FragmentRecordBatch::receiving_minor_fragment_id() const {
  return receiving_minor_fragment_id_;
}
inline ::google::protobuf::RepeatedField< ::google::protobuf::int32 >*
FragmentRecordBatch::mutable_receiving_minor_fragment_id() {
  return &receiving_minor_fragment_id_;
}

// optional int32 sending_major_fragment_id = 4;
inline bool FragmentRecordBatch::has_sending_major_fragment_id() const {
  return (_has_bits_[0] & 0x00000008u) != 0;
}
inline void FragmentRecordBatch::set_has_sending_major_fragment_id() {
  _has_bits_[0] |= 0x00000008u;
}
inline void FragmentRecordBatch::clear_has_sending_major_fragment_id() {
  _has_bits_[0] &= ~0x00000008u;
}
inline void FragmentRecordBatch::clear_sending_major_fragment_id() {
  sending_major_fragment_id_ = 0;
  clear_has_sending_major_fragment_id();
}
inline ::google::protobuf::int32 FragmentRecordBatch::sending_major_fragment_id() const {
  return sending_major_fragment_id_;
}
inline void FragmentRecordBatch::set_sending_major_fragment_id(::google::protobuf::int32 value) {
  set_has_sending_major_fragment_id();
  sending_major_fragment_id_ = value;
}

// optional int32 sending_minor_fragment_id = 5;
inline bool FragmentRecordBatch::has_sending_minor_fragment_id() const {
  return (_has_bits_[0] & 0x00000010u) != 0;
}
inline void FragmentRecordBatch::set_has_sending_minor_fragment_id() {
  _has_bits_[0] |= 0x00000010u;
}
inline void FragmentRecordBatch::clear_has_sending_minor_fragment_id() {
  _has_bits_[0] &= ~0x00000010u;
}
inline void FragmentRecordBatch::clear_sending_minor_fragment_id() {
  sending_minor_fragment_id_ = 0;
  clear_has_sending_minor_fragment_id();
}
inline ::google::protobuf::int32 FragmentRecordBatch::sending_minor_fragment_id() const {
  return sending_minor_fragment_id_;
}
inline void FragmentRecordBatch::set_sending_minor_fragment_id(::google::protobuf::int32 value) {
  set_has_sending_minor_fragment_id();
  sending_minor_fragment_id_ = value;
}

// optional .exec.shared.RecordBatchDef def = 6;
inline bool FragmentRecordBatch::has_def() const {
  return (_has_bits_[0] & 0x00000020u) != 0;
}
inline void FragmentRecordBatch::set_has_def() {
  _has_bits_[0] |= 0x00000020u;
}
inline void FragmentRecordBatch::clear_has_def() {
  _has_bits_[0] &= ~0x00000020u;
}
inline void FragmentRecordBatch::clear_def() {
  if (def_ != NULL) def_->::exec::shared::RecordBatchDef::Clear();
  clear_has_def();
}
inline const ::exec::shared::RecordBatchDef& FragmentRecordBatch::def() const {
  return def_ != NULL ? *def_ : *default_instance_->def_;
}
inline ::exec::shared::RecordBatchDef* FragmentRecordBatch::mutable_def() {
  set_has_def();
  if (def_ == NULL) def_ = new ::exec::shared::RecordBatchDef;
  return def_;
}
inline ::exec::shared::RecordBatchDef* FragmentRecordBatch::release_def() {
  clear_has_def();
  ::exec::shared::RecordBatchDef* temp = def_;
  def_ = NULL;
  return temp;
}
inline void FragmentRecordBatch::set_allocated_def(::exec::shared::RecordBatchDef* def) {
  delete def_;
  def_ = def;
  if (def) {
    set_has_def();
  } else {
    clear_has_def();
  }
}

// optional bool isLastBatch = 7;
inline bool FragmentRecordBatch::has_islastbatch() const {
  return (_has_bits_[0] & 0x00000040u) != 0;
}
inline void FragmentRecordBatch::set_has_islastbatch() {
  _has_bits_[0] |= 0x00000040u;
}
inline void FragmentRecordBatch::clear_has_islastbatch() {
  _has_bits_[0] &= ~0x00000040u;
}
inline void FragmentRecordBatch::clear_islastbatch() {
  islastbatch_ = false;
  clear_has_islastbatch();
}
inline bool FragmentRecordBatch::islastbatch() const {
  return islastbatch_;
}
inline void FragmentRecordBatch::set_islastbatch(bool value) {
  set_has_islastbatch();
  islastbatch_ = value;
}


// @@protoc_insertion_point(namespace_scope)

}  // namespace data
}  // namespace bit
}  // namespace exec

#ifndef SWIG
namespace google {
namespace protobuf {

template <>
inline const EnumDescriptor* GetEnumDescriptor< ::exec::bit::data::RpcType>() {
  return ::exec::bit::data::RpcType_descriptor();
}

}  // namespace google
}  // namespace protobuf
#endif  // SWIG

// @@protoc_insertion_point(global_scope)

#endif  // PROTOBUF_BitData_2eproto__INCLUDED
