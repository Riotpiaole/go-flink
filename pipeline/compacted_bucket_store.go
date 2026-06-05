package pipeline

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mapTaskDoc is the MongoDB document for the "map_task" collection.
type mapTaskDoc struct {
	ID        string `bson:"_id"`
	JobID     string `bson:"jobID"`
	PhaseUUID string `bson:"phaseUUID"`
	TaskID    int    `bson:"taskID"`
	ChunkID   string `bson:"chunkID"`
	FileName  string `bson:"fileName"`
}

// reduceTaskDoc is the MongoDB document for the "reduction" collection.
type reduceTaskDoc struct {
	ID        string `bson:"_id"`
	JobID     string `bson:"jobID"`
	PhaseUUID string `bson:"phaseUUID"`
	Bucket    int    `bson:"bucket"`
}

// sinkResultDoc is the MongoDB document for the "sink_result" collection.
// Written when a Sink bucket completes; auditable proof that the pipeline
// committed a bucket's output to a target collection.
type sinkResultDoc struct {
	ID             string `bson:"_id"`
	JobID          string `bson:"jobID"`
	PhaseUUID      string `bson:"phaseUUID"`
	Bucket         int    `bson:"bucket"`
	Database       string `bson:"database"`
	Collection     string `bson:"collection"`
	RecordsWritten int    `bson:"records_written"`
}

// CompactedBucketStore persists per-phase task completion state to MongoDB
// using four collections:
//
//   - "map_task"    — completed Map tasks (chunkID + fileName); lets
//     transitionToNextPhase reconstruct taskFiles after a leader failover.
//   - "reduction"   — completed Reduce buckets; AskForCompactTask dispatches
//     GroupBy tasks reactively as buckets finish.
//   - "compaction"  — atomic GroupBy dispatch claims; prevents two Compacter
//     pods from processing the same bucket concurrently.
//   - "sink_result" — completed Sink buckets with record counts; audit trail.
//
// Document _id format: "<jobID>:<phaseUUID>:<taskID-or-bucket>"
// phaseUUID (not phaseIdx) makes every record globally unique across re-runs.
//
// All methods degrade gracefully when the client is nil: writes are no-ops and
// reads return false/nil, preserving in-memory fallback paths in the coordinator.
type CompactedBucketStore struct {
	mu     sync.RWMutex
	jobID  string
	client *mongo.Client
}

// NewCompactedBucketStore returns a store. Call Connect and SetJobID before use.
func NewCompactedBucketStore(jobID string) *CompactedBucketStore {
	return &CompactedBucketStore{jobID: jobID}
}

// SetJobID updates the job scope. Write-locked so concurrent key() reads are safe.
func (s *CompactedBucketStore) SetJobID(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobID = jobID
}

// JobID returns the current job scope identifier.
func (s *CompactedBucketStore) JobID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.jobID
}

// Connect opens a persistent connection pool to uri.
// Falls back to mongodb://localhost:27017 when uri is empty.
// Safe to call more than once; subsequent calls are no-ops.
func (s *CompactedBucketStore) Connect(uri string) error {
	if s.client != nil {
		return nil
	}
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	client, err := mongo.Connect(mongoopts.Client().ApplyURI(uri))
	if err != nil {
		return fmt.Errorf("compacted-bucket-store: connect: %w", err)
	}
	s.client = client
	return nil
}

// Close disconnects from MongoDB. Safe to call when client is nil.
func (s *CompactedBucketStore) Close() {
	if s.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.client.Disconnect(ctx)
}

// MarkReduceDone records that bucket completed its Reduce phase for phaseUUID.
// Idempotent (duplicate-key errors are silently ignored).
func (s *CompactedBucketStore) MarkReduceDone(phaseUUID string, bucket int) {
	if s.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.db().Collection("reduction").InsertOne(ctx, reduceTaskDoc{
		ID:        s.key(phaseUUID, bucket),
		JobID:     s.JobID(),
		PhaseUUID: phaseUUID,
		Bucket:    bucket,
	})
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		log.Printf("[APP_METRIC] WARN MarkReduceDone bucket %d: %v", bucket, err)
	}
}

// IsReduceDone reports whether bucket has a completion record for phaseUUID.
func (s *CompactedBucketStore) IsReduceDone(phaseUUID string, bucket int) bool {
	if s.client == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.db().Collection("reduction").FindOne(ctx,
		bson.D{{Key: "_id", Value: s.key(phaseUUID, bucket)}}).Err()
	return err == nil
}

// ReduceTaskOutputs returns the bucket IDs of every Reduce task that completed
// for phaseUUID in this job. Returns (nil, nil) when the client is nil.
func (s *CompactedBucketStore) ReduceTaskOutputs(phaseUUID string) ([]int, error) {
	if s.client == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cur, err := s.db().Collection("reduction").Find(ctx, bson.D{
		{Key: "jobID", Value: s.JobID()},
		{Key: "phaseUUID", Value: phaseUUID},
	})
	if err != nil {
		return nil, fmt.Errorf("reduction find: %w", err)
	}
	defer cur.Close(ctx)
	var docs []reduceTaskDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("reduction decode: %w", err)
	}
	buckets := make([]int, len(docs))
	for i, d := range docs {
		buckets[i] = d.Bucket
	}
	return buckets, nil
}

// ClaimCompactDispatch atomically claims GroupBy dispatch for bucket in phaseUUID.
// The first caller receives true; all subsequent callers receive false.
func (s *CompactedBucketStore) ClaimCompactDispatch(phaseUUID string, bucket int) bool {
	if s.client == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.db().Collection("compaction").InsertOne(ctx,
		bson.D{{Key: "_id", Value: s.key(phaseUUID, bucket)}})
	return err == nil
}

// MarkMapTaskDone records that Map task taskID completed for phaseUUID.
// Stores chunkID and fileName so MapTaskOutputs can reconstruct taskFiles after
// a leader failover. Idempotent.
func (s *CompactedBucketStore) MarkMapTaskDone(phaseUUID string, taskID int, chunkID, fileName string) {
	if s.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.db().Collection("map_task").InsertOne(ctx, mapTaskDoc{
		ID:        s.key(phaseUUID, taskID),
		JobID:     s.JobID(),
		PhaseUUID: phaseUUID,
		TaskID:    taskID,
		ChunkID:   chunkID,
		FileName:  fileName,
	})
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		log.Printf("[APP_METRIC] WARN MarkMapTaskDone task %d: %v", taskID, err)
	}
}

// IsMapTaskDone reports whether Map task taskID has a completion record for phaseUUID.
func (s *CompactedBucketStore) IsMapTaskDone(phaseUUID string, taskID int) bool {
	if s.client == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.db().Collection("map_task").FindOne(ctx,
		bson.D{{Key: "_id", Value: s.key(phaseUUID, taskID)}}).Err()
	return err == nil
}

// MapTaskOutputs returns chunkID and fileName maps for every completed Map task
// in phaseUUID for this job, keyed by taskID. Used by transitionToNextPhase to
// reconstruct taskFiles after a leader failover.
// Returns (nil, nil, nil) when the client is nil.
func (s *CompactedBucketStore) MapTaskOutputs(phaseUUID string) (map[int]string, map[int]string, error) {
	if s.client == nil {
		return nil, nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cur, err := s.db().Collection("map_task").Find(ctx, bson.D{
		{Key: "jobID", Value: s.JobID()},
		{Key: "phaseUUID", Value: phaseUUID},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("map_task find: %w", err)
	}
	defer cur.Close(ctx)
	var docs []mapTaskDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, nil, fmt.Errorf("map_task decode: %w", err)
	}
	chunkIDs := make(map[int]string, len(docs))
	fileNames := make(map[int]string, len(docs))
	for _, d := range docs {
		chunkIDs[d.TaskID] = d.ChunkID
		fileNames[d.TaskID] = d.FileName
	}
	return chunkIDs, fileNames, nil
}

// MarkSinkDone records that Sink bucket completed, storing the target database,
// collection, and record count as an audit trail. Idempotent.
func (s *CompactedBucketStore) MarkSinkDone(phaseUUID string, bucket, recordsWritten int, database, collection string) {
	if s.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.db().Collection("sink_result").InsertOne(ctx, sinkResultDoc{
		ID:             s.key(phaseUUID, bucket),
		JobID:          s.JobID(),
		PhaseUUID:      phaseUUID,
		Bucket:         bucket,
		Database:       database,
		Collection:     collection,
		RecordsWritten: recordsWritten,
	})
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		log.Printf("[APP_METRIC] WARN MarkSinkDone bucket %d: %v", bucket, err)
	}
}

// key returns the document _id: "<jobID>:<phaseUUID>:<id>".
func (s *CompactedBucketStore) key(phaseUUID string, id int) string {
	s.mu.RLock()
	jobID := s.jobID
	s.mu.RUnlock()
	return fmt.Sprintf("%s:%s:%d", jobID, phaseUUID, id)
}

// db returns the pipeline database handle, reading MONGO_DB from the environment.
func (s *CompactedBucketStore) db() *mongo.Database {
	dbName := os.Getenv("MONGO_DB")
	if dbName == "" {
		dbName = "pipeline"
	}
	return s.client.Database(dbName)
}
