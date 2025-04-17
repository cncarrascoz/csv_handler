#pragma once

// Common gRPC includes needed for tests
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/server.h>
#include <grpcpp/impl/codegen/server_context.h>
#include <grpcpp/impl/codegen/client_context.h>
#include <grpcpp/impl/codegen/completion_queue.h>
#include <grpcpp/impl/codegen/status.h>
#include <grpcpp/impl/codegen/channel_interface.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/create_channel.h>

// Bring in commonly used gRPC types to avoid namespace qualifiers
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;
using grpc::CreateChannel;
using grpc::InsecureChannelCredentials;
using grpc::InsecureServerCredentials;
