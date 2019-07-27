package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	primitive "go.mongodb.org/mongo-driver/bson/primitive"
	mongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/rahulsingh/go-grpc-examples/blog/blogpb"
)

var collection *mongo.Collection

type server struct{}

// data-model object for blog
type blogItem struct {
	ID       primitive.ObjectID `bson:"_id,omitempty"`
	AuthorID string             `bson:"author_id"`
	Content  string             `bson:"content"`
	Title    string             `bson:"title"`
}

// CreateBlog is used to insert one record of blog into mongodb and return response and throws underlaying error
func (s *server) CreateBlog(ctx context.Context, req *blogpb.CreateBlogRequest) (*blogpb.CreateBlogResponse, error) {
	fmt.Println("Request received for creating a blog")

	// get the blog instance from request
	blog := req.GetBlog()

	// prepare data to insert into mongodb
	data := blogItem{
		AuthorID: blog.GetAuthorId(),
		Content:  blog.GetContent(),
		Title:    blog.GetTitle(),
	}

	// insert one record in mongo collection and pass underlaying error as grpc error code & status
	res, err := collection.InsertOne(context.Background(), data)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Internal Error: %v", err),
		)
	}

	// if successfully inserted the get the objectID created and caste it to objectID
	oid, ok := res.InsertedID.(primitive.ObjectID)
	if !ok {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("cannot convert to objectID"),
		)
	}

	// return blog instance with blogID
	return &blogpb.CreateBlogResponse{
		Blog: &blogpb.Blog{
			Id:       oid.Hex(),
			AuthorId: blog.GetAuthorId(),
			Content:  blog.GetContent(),
			Title:    blog.GetTitle(),
		},
	}, nil

}

// ReadBlog fetch the Blog from mongodb for given blogID and return NOT_FOUND error if blog is not found in db
func (s *server) ReadBlog(ctx context.Context, req *blogpb.ReadBlogRequest) (*blogpb.ReadBlogResponse, error) {
	fmt.Println("Request received for ReadBlog")

	// read blogId from request and parse it as ObjectID
	blogID := req.GetBlogId()
	oid, err := primitive.ObjectIDFromHex(blogID)
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("Cannot parse blog ID: %v", err),
		)
	}

	// create an empty struct and create a filter for oid
	data := &blogItem{}
	filter := bson.M{"_id": oid}

	// query mongodb with given blogID and parse the response into a struct
	res := collection.FindOne(context.Background(), filter)
	if err := res.Decode(data); err != nil {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintf("Cannot find blog with given ID: %v", err),
		)
	}

	// return success response with Blog object
	return &blogpb.ReadBlogResponse{
		Blog: &blogpb.Blog{
			Id:       data.ID.Hex(),
			AuthorId: data.AuthorID,
			Title:    data.Title,
			Content:  data.Title,
		},
	}, nil

}

// UpdateBlog updated the given blog in mongodb and return updated blog instance
// throws NOT_FOUND error if blog is not found in mongodb
func (s *server) UpdateBlog(ctx context.Context, req *blogpb.UpdateBlogRequest) (*blogpb.UpdateBlogResponse, error) {
	fmt.Println("Request received for UpdateBlog")

	// read blog from request and fetch blogID from it and parse it as ObjectID
	blog := req.GetBlog()
	oid, err := primitive.ObjectIDFromHex(blog.GetId())
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("Cannot parse blog ID: %v", err),
		)
	}
	// create an empty struct and create a filter for oid
	data := &blogItem{}
	filter := bson.M{"_id": oid}

	// query mongodb with given blogID and parse the response into a struct
	res := collection.FindOne(context.Background(), filter)
	if err := res.Decode(data); err != nil {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintf("Cannot find blog with given ID: %v", err),
		)
	}

	// we update our internal struct data
	data.AuthorID = blog.GetAuthorId()
	data.Title = blog.GetTitle()
	data.Content = blog.GetContent()

	// update blog document
	_, updateErr := collection.ReplaceOne(context.Background(), filter, data)
	if updateErr != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Cannot update object in mongodb: %v", updateErr),
		)
	}

	return &blogpb.UpdateBlogResponse{
		Blog: &blogpb.Blog{
			Id:       data.ID.Hex(),
			AuthorId: data.AuthorID,
			Title:    data.Title,
			Content:  data.Title,
		},
	}, nil

}

// DeleteBlog takes a blogID and delete blog document from mongodb and return successfully deleted blogID
// Throws underlaying error and NOT_FOUND if blog does not found in mongodb for gievn blogID
func (s *server) DeleteBlog(ctx context.Context, req *blogpb.DeleteBlogRequest) (*blogpb.DeleteBlogResponse, error) {
	fmt.Println("Request received for DeleteBlog")

	// read blogId from request and parse it as ObjectID
	blogID := req.GetBlogId()
	oid, err := primitive.ObjectIDFromHex(blogID)
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			fmt.Sprintf("Cannot parse blogID: %v", err),
		)
	}

	// delete filter with blogId
	filter := bson.M{"_id": oid}

	// delete documnet from mongodb
	res, err := collection.DeleteOne(context.Background(), filter)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			fmt.Sprintf("Cannot delete object from mongodb: %v", err),
		)
	}

	// check if any item is deleted from mongodb or not
	if res.DeletedCount == 0 {
		return nil, status.Errorf(
			codes.NotFound,
			fmt.Sprintf("Cannot find blog in mongodb"),
		)
	}

	// successfully deleted blog
	return &blogpb.DeleteBlogResponse{
		BlogId: blogID,
	}, nil
}

// ListBlog used to stream list of all blogs from mongodb
// throws underlaying error in case of any error
func (s *server) ListBlog(req *blogpb.ListBlogRequest, stream blogpb.BlogService_ListBlogServer) error {
	fmt.Println("Request received for ListBlog Streaming ****")

	// get the cursor for list of all blogs in mongodb
	cur, err := collection.Find(context.Background(), bson.D{}) // return list of all blogs in mongodb
	if err != nil {
		return status.Errorf(
			codes.Internal,
			fmt.Sprintf("Unknow internal error from mongodb: %v", err),
		)
	}

	// when function exist then cursor will be closed
	defer cur.Close(context.Background())

	// iterate over cursor and find all blog elements
	// decode blog and stream the response
	for cur.Next(context.Background()) {
		data := &blogItem{}
		err := cur.Decode(data)
		if err != nil {
			return status.Errorf(
				codes.Internal,
				fmt.Sprintf("error while decoding data from mongodb: %v", err),
			)
		}

		// stream the response
		blog := &blogpb.Blog{
			Id:       data.ID.Hex(),
			AuthorId: data.AuthorID,
			Title:    data.Title,
			Content:  data.Title,
		}
		stream.Send(&blogpb.ListBlogResponse{Blog: blog})
	}

	// check for any unknown error from cursor
	if err := cur.Err(); err != nil {
		return status.Errorf(
			codes.Internal,
			fmt.Sprintf("Unknow internal error: %v", err),
		)
	}

	return nil
}

func main() {
	// stetting the log level ,if we crash go code we get the file name and line number
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	fmt.Println("**** Connecting to mongodb *****")

	// Connect to mongodb : client is connection object to mongodb
	// create a new client and start monitoring the MongoDB server on localhost
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("error while creating mongo-cleint: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatalf("error while creating mongo-cleint-ctx: %v", err)
	}

	// Database and Collection types can be used to access the database in mongodb
	// if database "mydb" does not exist then it will be created
	// connect to database "mydb" and the use collection "blog"
	// make collection object as global variable to access it from outside this file
	collection = client.Database("mydb").Collection("blog")

	fmt.Println("**** GRPC SERVER Setup-Blog Server *****")

	// Create TCP connection and do port binding
	// Default port for grpc server is 50051
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Failed to listen tcp: %v", err)
	}

	// Create GRPC server and register gRPC serveice with it
	opts := []grpc.ServerOption{}
	s := grpc.NewServer(opts...)
	blogpb.RegisterBlogServiceServer(s, &server{})

	// Register server with grpc-reflection
	reflection.Register(s)

	// Registering as shutdown hook for graceful shutdown of server using go-routine
	go func() {
		fmt.Println("Starting the server")
		// Bind port with grpc server
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Failed to server: %v", err)
		}
	}()

	// wait for control+C for exit
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	// Block until a signal is received (control+c)
	<-ch
	fmt.Println("Stopping the server")
	s.Stop()
	fmt.Println("Closing the listener")
	lis.Close()
	fmt.Println("Closing mongodb connection")
	client.Disconnect(context.TODO())
	fmt.Println("Server gracefully shutdown..End of program")

}
