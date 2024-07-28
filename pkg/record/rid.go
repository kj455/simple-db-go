package record

import "fmt"

type RIDImpl struct {
	blknum int
	slot   int
}

// NewRID creates a new RID for the record having the
// specified location in the specified block.
func NewRID(blknum, slot int) RID {
	return &RIDImpl{blknum: blknum, slot: slot}
}

// BlockNumber returns the block number associated with this RID.
func (rid *RIDImpl) BlockNumber() int {
	return rid.blknum
}

// Slot returns the slot associated with this RID.
func (rid *RIDImpl) Slot() int {
	return rid.slot
}

// Equals compares this RID with another RID for equality.
func (rid *RIDImpl) Equals(other RID) bool {
	return rid.blknum == other.BlockNumber() && rid.slot == other.Slot()
}

// String returns the string representation of the RID.
func (rid *RIDImpl) String() string {
	return "[" + fmt.Sprintf("%d", rid.blknum) + ", " + fmt.Sprintf("%d", rid.slot) + "]"
}
