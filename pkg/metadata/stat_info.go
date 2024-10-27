package metadata

// StatInfoImpl holds statistical information about a table:
// the number of blocks, the number of records,
// and the number of distinct values for each field.
type StatInfoImpl struct {
	numBlocks  int
	numRecords int
}

// NewStatInfo creates a StatInfoImpl object.
// Note that the number of distinct values is not
// passed into the constructor.
// The function fakes this value.
func NewStatInfo(numBlocks, numRecords int) StatInfo {
	return &StatInfoImpl{
		numBlocks:  numBlocks,
		numRecords: numRecords,
	}
}

// BlocksAccessed returns the estimated number of blocks in the table.
func (si *StatInfoImpl) BlocksAccessed() int {
	return si.numBlocks
}

// RecordsOutput returns the estimated number of records in the table.
func (si *StatInfoImpl) RecordsOutput() int {
	return si.numRecords
}

// DistinctValues returns the estimated number of distinct values
// for the specified field.
// This estimate is a complete guess, because doing something
// reasonable is beyond the scope of this system.
func (si *StatInfoImpl) DistinctValues(fldname string) int {
	return 1 + (si.numRecords / 3)
}
