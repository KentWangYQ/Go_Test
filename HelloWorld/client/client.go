package main

import (
	"io"
	"log"
	pb "test/grpc_test/storage"

	"google.golang.org/grpc"

	"golang.org/x/net/context"
)

func printFirstData(client pb.StorageClient, user *pb.User) {
	d, err := client.GetFirstData(context.Background(), user)
	if err != nil {
		log.Fatalf("Client: GetData() error: %v", err)
	}
	log.Println(d)
}

func printData(client pb.StorageClient, user *pb.User) {
	stream, err := client.GetData(context.Background(), user)
	if err != nil {
		log.Fatalf("Client: GetData() error: %v", err)
	}
	for {
		d, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Client: GetData().Recv() error: %v", err)
		}
		log.Println(d)
	}
}

func insertData(client pb.StorageClient) {
	user := &pb.User{Id: 1, Name: "Kent"}
	dataArr := []*pb.Data{
		&pb.Data{User: user, Msg: "i_msg_1"},
		&pb.Data{User: user, Msg: "i_msg_2"},
		&pb.Data{User: user, Msg: "i_msg_3"},
		&pb.Data{User: user, Msg: "i_msg_4"},
	}
	stream, err := client.InsertData(context.Background())
	if err != nil {
		log.Fatalf("Client: InsertData() error: %v", err)
	}

	for _, sd := range dataArr {
		err := stream.Send(sd)
		if err != nil {
			log.Fatalf("Client: InsertData().Send() error: %v", err)
		}
	}

	ds, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Client: InsertData().CloseAndRevc() error: %v", err)
	}
	log.Println(ds)
}

func insertAndShowAllData(client pb.StorageClient) {
	user := &pb.User{Id: 1, Name: "Kent"}
	dataArr := []*pb.Data{
		&pb.Data{User: user, Msg: "ia_msg_1"},
		&pb.Data{User: user, Msg: "ia_msg_2"},
		&pb.Data{User: user, Msg: "ia_msg_3"},
		&pb.Data{User: user, Msg: "ia_msg_4"},
	}

	stream, err := client.InsertAndShowAllData(context.Background())

	if err != nil {
		log.Fatalf("Client: IAData() error: %v", err)
	}

	waitc := make(chan struct{})

	go func() {
		for {
			d, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("Client: IAData().Recv() error: %v", err)
			}
			log.Println(d)
		}
	}()

	for _, d := range dataArr {
		err := stream.Send(d)
		if err != nil {
			log.Fatalf("Client: IAData().Send() error: %v", err)
		}
	}
	stream.CloseSend()
	<-waitc
}

func main() {
	conn, err := grpc.Dial("localhost:55001", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Client: Create connection error: %v", err)
	}
	defer conn.Close()
	client := pb.NewStorageClient(conn)

	insertData(client)
	insertAndShowAllData(client)

	user := &pb.User{Id: 1, Name: "Kent"}
	printFirstData(client, user)
	printData(client, user)
}
