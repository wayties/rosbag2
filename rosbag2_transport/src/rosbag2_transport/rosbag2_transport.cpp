// Copyright 2018, Bosch Software Innovations GmbH.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "rosbag2_transport/rosbag2_transport.hpp"

#include <memory>
#include <queue>
#include <string>
#include <utility>
#include <vector>

#include "rclcpp/rclcpp.hpp"
#include "rcutils/time.h"

#include "rosbag2_transport/logging.hpp"
#include "rosbag2/info.hpp"
#include "rosbag2/sequential_reader.hpp"
#include "rosbag2/types.hpp"
#include "rosbag2/typesupport_helpers.hpp"
#include "rosbag2/writer.hpp"

#include "formatter.hpp"
#include "player.hpp"
#include "recorder.hpp"
#include "rosbag2_node.hpp"

namespace rosbag2_transport
{

Rosbag2Transport::Rosbag2Transport()
: reader_(std::make_shared<rosbag2::SequentialReader>()),
  writer_(std::make_shared<rosbag2::Writer>()),
  info_(std::make_shared<rosbag2::Info>())
{}

Rosbag2Transport::Rosbag2Transport(
  std::shared_ptr<rosbag2::SequentialReader> reader,
  std::shared_ptr<rosbag2::Writer> writer,
  std::shared_ptr<rosbag2::Info> info)
: reader_(std::move(reader)), writer_(std::move(writer)), info_(std::move(info)) {}

void Rosbag2Transport::init()
{
  rclcpp::init(0, nullptr);
}

void Rosbag2Transport::shutdown()
{
  rclcpp::shutdown();
}

void Rosbag2Transport::record(
  const StorageOptions & storage_options, const RecordOptions & record_options)
{
  try {
    writer_->open(
      storage_options, {rmw_get_serialization_format(), record_options.rmw_serialization_format});

    auto transport_node = setup_node(record_options.node_prefix);

    Recorder recorder(writer_, transport_node);
    recorder.record(record_options);
  } catch (std::runtime_error & e) {
    ROSBAG2_TRANSPORT_LOG_ERROR("Failed to record: %s", e.what());
  }
}

std::shared_ptr<Rosbag2Node> Rosbag2Transport::setup_node(std::string node_prefix)
{
  if (!transport_node_) {
    transport_node_ = std::make_shared<Rosbag2Node>(node_prefix + "_rosbag2");
  }
  return transport_node_;
}

void Rosbag2Transport::play(
  const StorageOptions & storage_options, const PlayOptions & play_options)
{
  try {
    reader_->open(storage_options, {"", rmw_get_serialization_format()});

    auto transport_node = setup_node(play_options.node_prefix);

    Player player(reader_, transport_node);
    player.play(play_options);
  } catch (std::runtime_error & e) {
    ROSBAG2_TRANSPORT_LOG_ERROR("Failed to play: %s", e.what());
  }
}

void Rosbag2Transport::print_bag_info(const std::string & uri, const std::string & storage_id)
{
  rosbag2::BagMetadata metadata;
  try {
    metadata = info_->read_metadata(uri, storage_id);
  } catch (std::runtime_error & e) {
    (void) e;
    ROSBAG2_TRANSPORT_LOG_ERROR_STREAM("Could not read metadata for " << uri << ". Please specify "
      "the path to the folder containing an existing 'metadata.yaml' file or provide correct "
      "storage id if metadata file doesn't exist (see help).");
    return;
  }

  Formatter::format_bag_meta_data(metadata);
}

bool Rosbag2Transport::open(const StorageOptions & storage_options)
{
  try {
    reader_->open(storage_options, {"", rmw_get_serialization_format()});
    return true;
  } catch (std::runtime_error & e) {
    ROSBAG2_TRANSPORT_LOG_ERROR("Failed to open: %s", e.what());
    return false;
  }
}

std::shared_ptr<rosbag2_introspection_message_t> Rosbag2Transport::next()
{
  try {
    if (reader_->has_next()) {
      return reader_->read_next_deserialized();
    }
    return nullptr;
  } catch (std::runtime_error & e) {
    ROSBAG2_TRANSPORT_LOG_ERROR("Failed to get next: %s", e.what());
    return nullptr;
  }
}

}  // namespace rosbag2_transport
