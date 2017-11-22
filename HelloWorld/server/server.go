package main

import (
	"io"
	"log"
	"net"
	pb "github.com/kentwangyq/grpc_test/helloworld/storage"

	"google.golang.org/grpc"

	"golang.org/x/net/context"
)

type storeServer struct {
	Datas map[int32][]*pb.Data
}

func (s *storeServer) GetFirstData(ctx context.Context, user *pb.User) (*pb.Data, error) {
	if ds, ok := s.Datas[user.Id]; ok {
		if len(ds) > 0 {
			return ds[0], nil
		}
	}
	return nil, nil
}

func (s *storeServer) GetData(user *pb.User, stream pb.Storage_GetDataServer) error {
	if ds, ok := s.Datas[user.Id]; ok {
		for _, d := range ds {
			if err := stream.Send(d); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *storeServer) InsertData(stream pb.Storage_InsertDataServer) error {
	var msgCount int32
	for {
		d, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pb.DataSummary{
				MessageCount: msgCount,
			})

		}
		if err != nil {
			return err
		}
		s.Datas[d.User.Id] = append(s.Datas[d.User.Id], d)
		msgCount++
	}
}

func (s *storeServer) InsertAndShowAllData(stream pb.Storage_InsertAndShowAllDataServer) error {
	for {
		rd, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		s.Datas[rd.User.Id] = append(s.Datas[rd.User.Id], rd)

		for _, d := range s.Datas[rd.User.Id] {
			if err := stream.Send(d); err != nil {
				return err
			}
		}
	}
}

func main() {
	lis, err := net.Listen("tcp", "localhost:55001")
	if err != nil {
		log.Fatalf("Listener create error: %v", err)
	}
	grpcServer := grpc.NewServer()
	storageServer := &storeServer{Datas: make(map[int32][]*pb.Data)}
	pb.RegisterStorageServer(grpcServer, storageServer)
	grpcServer.Serve(lis)
}
