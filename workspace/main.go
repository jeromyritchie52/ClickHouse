package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Row represents a row in our tables: (id UInt64, val String)
type Row struct {
	ID  uint64
	Val string
}

// Size returns the approximate memory size of the row in bytes
func (r Row) Size() int64 {
	return 8 + int64(len(r.Val))
}

// Settings defines the query execution settings
type Settings struct {
	MaxMemoryUsage int64
	MaxBytesInJoin int64
	JoinAlgorithm  string // "hash", "grace_hash", "auto"
}

// QueryContext represents the execution context
type QueryContext struct {
	Settings Settings
}

// HashJoin manages the join operation
type HashJoin struct {
	ctx          *QueryContext
	memTable     map[uint64]string
	memUsage     int64
	isSpilled    bool
	tempFiles    []*os.File
	spilledBytes int64
}

func NewHashJoin(ctx *QueryContext) *HashJoin {
	return &HashJoin{
		ctx:      ctx,
		memTable: make(map[uint64]string),
	}
}

// Close cleans up any temporary files
func (hj *HashJoin) Close() {
	for _, f := range hj.tempFiles {
		f.Close()
		os.Remove(f.Name())
	}
}

// InsertRow adds a row from the right-side table to the join structure
func (hj *HashJoin) InsertRow(row Row) error {
	rowSize := row.Size()

	if !hj.isSpilled {
		// Check if adding this row exceeds the memory limit
		if hj.memUsage+rowSize > hj.ctx.Settings.MaxBytesInJoin {
			if hj.ctx.Settings.JoinAlgorithm == "auto" {
				// Transition to disk-spilling (Grace Hash Join simulation)
				hj.isSpilled = true
				if err := hj.spillMemoryTableToDisk(); err != nil {
					return fmt.Errorf("failed to spill memory table to disk: %w", err)
				}
			} else {
				return fmt.Errorf("memory limit exceeded: max_bytes_in_join limit of %d bytes reached", hj.ctx.Settings.MaxBytesInJoin)
			}
		}
	}

	if hj.isSpilled {
		return hj.spillRowToDisk(row)
	}

	hj.memTable[row.ID] = row.Val
	hj.memUsage += rowSize
	return nil
}

func (hj *HashJoin) spillMemoryTableToDisk() error {
	tmpFile, err := os.CreateTemp("", "clickhouse_join_spill_")
	if err != nil {
		return err
	}
	hj.tempFiles = append(hj.tempFiles, tmpFile)

	for id, val := range hj.memTable {
		if err := writeRowToDisk(tmpFile, Row{ID: id, Val: val}); err != nil {
			return err
		}
		hj.spilledBytes += Row{ID: id, Val: val}.Size()
	}

	// Clear memory table to free memory
	hj.memTable = make(map[uint64]string)
	hj.memUsage = 0
	return nil
}

func (hj *HashJoin) spillRowToDisk(row Row) error {
	var targetFile *os.File
	if len(hj.tempFiles) == 0 {
		tmpFile, err := os.CreateTemp("", "clickhouse_join_spill_")
		if err != nil {
			return err
		}
		hj.tempFiles = append(hj.tempFiles, tmpFile)
		targetFile = tmpFile
	} else {
		targetFile = hj.tempFiles[len(hj.tempFiles)-1]
	}

	if err := writeRowToDisk(targetFile, row); err != nil {
		return err
	}
	hj.spilledBytes += row.Size()
	return nil
}

func writeRowToDisk(w io.Writer, row Row) error {
	if err := binary.Write(w, binary.LittleEndian, row.ID); err != nil {
		return err
	}
	valBytes := []byte(row.Val)
	if err := binary.Write(w, binary.LittleEndian, int32(len(valBytes))); err != nil {
		return err
	}
	_, err := w.Write(valBytes)
	return err
}

func readRowFromDisk(r io.Reader) (Row, error) {
	var id uint64
	if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
		return Row{}, err
	}
	var length int32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return Row{}, err
	}
	valBytes := make([]byte, length)
	if _, err := io.ReadFull(r, valBytes); err != nil {
		return Row{}, err
	}
	return Row{ID: id, Val: string(valBytes)}, nil
}

// Join performs the join operation with the left-side row
func (hj *HashJoin) Join(leftRow Row) (string, bool, error) {
	if !hj.isSpilled {
		val, found := hj.memTable[leftRow.ID]
		return val, found, nil
	}

	// If spilled, we scan the temporary files (simulating a disk-based lookup/join)
	for _, f := range hj.tempFiles {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return "", false, err
		}
		for {
			row, err := readRowFromDisk(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				return "", false, err
			}
			if row.ID == leftRow.ID {
				return row.Val, true, nil
			}
		}
	}
	return "", false, nil
}

// DistributedJoin simulates a distributed join across shards
func DistributedJoin(shards [][]Row, leftTable []Row, settings Settings) (int, int64, error) {
	ctx := &QueryContext{Settings: settings}
	hj := NewHashJoin(ctx)
	defer hj.Close()

	// Populate right-side table from shards
	for _, shard := range shards {
		for _, row := range shard {
			if err := hj.InsertRow(row); err != nil {
				return 0, 0, err
			}
		}
	}

	// Perform join
	matchCount := 0
	for _, leftRow := range leftTable {
		_, found, err := hj.Join(leftRow)
		if err != nil {
			return 0, 0, err
		}
		if found {
			matchCount++
		}
	}

	return matchCount, hj.spilledBytes, nil
}

func main() {
	fmt.Println("Starting Distributed JOIN Memory Optimization Simulation...")

	// 1. Setup mock data
	// Right-side table (to be joined)
	shard1 := []Row{
		{ID: 1, Val: "Value1"},
		{ID: 2, Val: "Value2"},
	}
	shard2 := []Row{
		{ID: 3, Val: "Value3"},
		{ID: 4, Val: "Value4"},
	}
	shards := [][]Row{shard1, shard2}

	// Left-side table
	leftTable := []Row{
		{ID: 2, Val: "Left2"},
		{ID: 4, Val: "Left4"},
		{ID: 5, Val: "Left5"},
	}

	// Test Case 1: In-memory join within limits
	settingsNormal := Settings{
		MaxMemoryUsage: 1024 * 1024,
		MaxBytesInJoin: 1024 * 1024,
		JoinAlgorithm:  "auto",
	}
	matches, spilled, err := DistributedJoin(shards, leftTable, settingsNormal)
	if err != nil {
		fmt.Printf("Test 1 Failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Test 1 (In-Memory): Matches = %d, Spilled Bytes = %d\n", matches, spilled)

	// Test Case 2: Join exceeding limits with JoinAlgorithm = "auto" (should spill to disk)
	settingsSpill := Settings{
		MaxMemoryUsage: 1024 * 1024,
		MaxBytesInJoin: 20, // Very low limit to force spilling
		JoinAlgorithm:  "auto",
	}
	matchesSpill, spilledBytes, err := DistributedJoin(shards, leftTable, settingsSpill)
	if err != nil {
		fmt.Printf("Test 2 Failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Test 2 (Spilled to Disk): Matches = %d, Spilled Bytes = %d\n", matchesSpill, spilledBytes)

	// Test Case 3: Join exceeding limits with JoinAlgorithm = "hash" (should fail)
	settingsFail := Settings{
		MaxMemoryUsage: 1024 * 1024,
		MaxBytesInJoin: 20,
		JoinAlgorithm:  "hash",
	}
	_, _, err = DistributedJoin(shards, leftTable, settingsFail)
	if err == nil {
		fmt.Println("Test 3 Failed: Expected memory limit error, but query succeeded")
		os.Exit(1)
	}
	fmt.Printf("Test 3 (Expected Failure): Successfully caught error: %v\n", err)

	fmt.Println("All tests passed successfully!")
}
