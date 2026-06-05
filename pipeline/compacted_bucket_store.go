package pipeline

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mapTaskDoc is the MongoDB document schema for the "map_task" collection.
type mapTaskDoc struct {
	ID       string `bson:"_id"`
	JobID    string `bson:"jobID"`
	PhaseIdx int    `bson:"phaseIdx"`
	TaskID   int    `bson:"taskID"`
	ChunkID  string `bson:"chunkID"`
	FileName string `bson:"fileName"`
}

// reduceTaskDoc is the MongoDB document schema for the "reduction" collection.
type reduceTaskDoc struct {
	ID       string `bson:"_id"`
	JobID    string `bson:"jobID"`
	PhaseIdx int    `bson:"phaseIdx"`
	Bucket   int    `bson:"bucket"`
}

// CompactedBucketStore persists per-phase task completion state to MongoDB
// using three collections:
//
//   - "map_task"   — completed Map tasks with chunkID and fileName; lets
//     transitionToNextPhase reconstruct taskFiles after a leader failover.
//   - "reduction"  — completed Reduce buckets; lets AskForCompactTask dispatch
//     GroupBy tasks reactively as buckets finish.
//   - "compaction" — atomic GroupBy dispatch claims; prevents two compacter pods
//     from processing the same bucket concurrently.
//
// Document keys are "<jobID>:<phaseIdx>:<id>" so records are scoped to a
// specific job AND Raft phase. jobID is stable across leader failovers because
// it is replicated in the Raft snapshot (set via SetJobID from Restore).
//
// All methods degrade gracefully when the client is nil: writes are no-ops and
// reads return false/nil, preserving in-memory fallback paths in the coordinator.
type CompactedBucketStore struct {
	jobID  string
	client *mongo.Client
}

// NewCompactedBucketStore returns a store scoped to jobID.
// Call Connect before using any other method.
func NewCompactedBucketStore(jobID string) *CompactedBucketStore {
	return &CompactedBucketStore{jobID: jobID}
}

// SetJobID updates the job scope. Called from coordinator.SubmitJob (on job
// acceptance) and from FSM.Restore (after a leader failover replays a snapshot).
func (s *CompactedBucketStore) SetJobID(jobID string) {
	s.jobID = jobID
}

// JobID returns the current job scope identifier.
func (s *CompactedBucketStore) JobID() string {
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

// MarkReduceDone records that bucket in phaseIdx completed its Reduce phase.
// Stores jobID, phaseIdx, and bucket so ReduceTaskOutputs can enumerate
// completed buckets for a phase. Idempotent.
func (s *CompactedBucketStore) MarkReduceDone(phaseIdx, bucket int) {
	if s.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.db().Collection("reduction").InsertOne(ctx, reduceTaskDoc{
		ID:       s.key(phaseIdx, bucket),
		JobID:    s.jobID,
		PhaseIdx: phaseIdx,
		Bucket:   bucket,
	})
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		appLog("WARN", "MarkReduceDone bucket %d: %v", bucket, err)
	}
}

// IsReduceDone reports whether bucket in phaseIdx has a completion record.
// Returns false when the client is nil.
func (s *CompactedBucketStore) IsReduceDone(phaseIdx, bucket int) bool {
	if s.client == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.db().Collection("reduction").FindOne(ctx,
		bson.D{{Key: "_id", Value: s.key(phaseIdx, bucket)}}).Err()
	return err == nil
}

// ReduceTaskOutputs returns the bucket IDs of every Reduce task that completed
// in phaseIdx for this job. Returns (nil, nil) when the client is nil.
func (s *CompactedBucketStore) ReduceTaskOutputs(phaseIdx int) ([]int, error) {
	if s.client == nil {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cur, err := s.db().Collection("reduction").Find(ctx, bson.D{
		{Key: "jobID", Value: s.jobID},
		{Key: "phaseIdx", Value: phaseIdx},
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

// ClaimCompactDispatch atomically claims GroupBy dispatch for bucket in phaseIdx.
// Uses MongoDB insert uniqueness as the atomic primitive: the first caller
// succeeds and receives true; all subsequent callers receive false.
// Keys include jobID so claims are scoped to this job and survive leader failover.
func (s *CompactedBucketStore) ClaimCompactDispatch(phaseIdx, bucket int) bool {
	if s.client == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.db().Collection("compaction").InsertOne(ctx,
		bson.D{{Key: "_id", Value: s.key(phaseIdx, bucket)}})
	return err == nil
}

// MarkMapTaskDone records that Map task taskID in phaseIdx completed, storing
// chunkID and fileName so MapTaskOutputs can reconstruct taskFiles after a
// leader failover. Idempotent.
func (s *CompactedBucketStore) MarkMapTaskDone(phaseIdx, taskID int, chunkID, fileName string) {
	if s.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := s.db().Collection("map_task").InsertOne(ctx, mapTaskDoc{
		ID:       s.key(phaseIdx, taskID),
		JobID:    s.jobID,
		PhaseIdx: phaseIdx,
		TaskID:   taskID,
		ChunkID:  chunkID,
		FileName: fileName,
	})
	if err != nil && !mongo.IsDuplicateKeyError(err) {
		appLog("WARN", "MarkMapTaskDone task %d: %v", taskID, err)
	}
}

// IsMapTaskDone reports whether Map task taskID in phaseIdx has a completion
// record. Returns false when the client is nil.
func (s *CompactedBucketStore) IsMapTaskDone(phaseIdx, taskID int) bool {
	if s.client == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.db().Collection("map_task").FindOne(ctx,
		bson.D{{Key: "_id", Value: s.key(phaseIdx, taskID)}}).Err()
	return err == nil
}

// MapTaskOutputs returns chunkID and fileName maps for every completed Map task
// in phaseIdx for this job, keyed by taskID. Used by transitionToNextPhase to
// reconstruct taskFiles after a leader failover.
// Returns (nil, nil, nil) when the client is nil — caller falls back to in-memory taskFiles.
func (s *CompactedBucketStore) MapTaskOutputs(phaseIdx int) (map[int]string, map[int]string, error) {
	if s.client == nil {
		return nil, nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cur, err := s.db().Collection("map_task").Find(ctx, bson.D{
		{Key: "jobID", Value: s.jobID},
		{Key: "phaseIdx", Value: phaseIdx},
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

// key returns the document _id scoped to this job, phaseIdx, and id.
// Format: "<jobID>:<phaseIdx>:<id>"
func (s *CompactedBucketStore) key(phaseIdx, id int) string {
	return fmt.Sprintf("%s:%d:%d", s.jobID, phaseIdx, id)
}

// db returns the pipeline database handle, reading MONGO_DB from the environment.
func (s *CompactedBucketStore) db() *mongo.Database {
	dbName := os.Getenv("MONGO_DB")
	if dbName == "" {
		dbName = "pipeline"
	}
	return s.client.Database(dbName)
}

