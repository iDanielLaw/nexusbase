package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

const (
	IndexMagic    = 0xBAAAD700
	IndexVersion  = 1
	tocEntryCount = 6
)

// TOC contains offsets to primary sections in the index file.
// Offsets are file byte offsets. A zero value means the section is absent.
type TOC struct {
	Symbols             uint64
	Series              uint64
	LabelIndicesStart   uint64
	LabelOffsetTable    uint64
	PostingsStart       uint64
	PostingsOffsetTable uint64
}

// ------------------------------------------------------------------
// Header / TOC helpers
// ------------------------------------------------------------------

// WriteTOC writes the TOC and a trailing CRC32 to the writer. It writes the
// 6 uint64 entries (little-endian) followed by CRC32 over those 48 bytes.
func WriteTOC(w io.Writer, toc *TOC) error {
	buf := make([]byte, 8*tocEntryCount)
	binary.LittleEndian.PutUint64(buf[0:], toc.Symbols)
	binary.LittleEndian.PutUint64(buf[8:], toc.Series)
	binary.LittleEndian.PutUint64(buf[16:], toc.LabelIndicesStart)
	binary.LittleEndian.PutUint64(buf[24:], toc.LabelOffsetTable)
	binary.LittleEndian.PutUint64(buf[32:], toc.PostingsStart)
	binary.LittleEndian.PutUint64(buf[40:], toc.PostingsOffsetTable)

	if _, err := w.Write(buf); err != nil {
		return fmt.Errorf("write toc: %w", err)
	}
	crc := crc32.ChecksumIEEE(buf)
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	if _, err := w.Write(crcBuf); err != nil {
		return fmt.Errorf("write toc crc: %w", err)
	}
	return nil
}

// ReadTOC reads the TOC from the end of the given file.
func ReadTOC(path string) (*TOC, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	const tocSize = 8 * tocEntryCount
	const crcSize = 4

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < int64(tocSize+crcSize) {
		return nil, io.ErrUnexpectedEOF
	}

	if _, err := f.Seek(-int64(tocSize+crcSize), io.SeekEnd); err != nil {
		return nil, err
	}
	buf := make([]byte, tocSize)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}
	crcBuf := make([]byte, crcSize)
	if _, err := io.ReadFull(f, crcBuf); err != nil {
		return nil, err
	}
	got := binary.LittleEndian.Uint32(crcBuf)
	want := crc32.ChecksumIEEE(buf)
	if got != want {
		return nil, fmt.Errorf("toc checksum mismatch: got=%08x want=%08x", got, want)
	}

	toc := &TOC{
		Symbols:             binary.LittleEndian.Uint64(buf[0:8]),
		Series:              binary.LittleEndian.Uint64(buf[8:16]),
		LabelIndicesStart:   binary.LittleEndian.Uint64(buf[16:24]),
		LabelOffsetTable:    binary.LittleEndian.Uint64(buf[24:32]),
		PostingsStart:       binary.LittleEndian.Uint64(buf[32:40]),
		PostingsOffsetTable: binary.LittleEndian.Uint64(buf[40:48]),
	}
	return toc, nil
}

// CreateIndexWithSections writes a file with header, symbol table, series section and TOC.
// It returns an error on failure. This is a convenience writer used by tests and tooling.
func CreateIndexWithSections(path string, symbols []string, series []SeriesEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// header: magic(4) + version(1)
	hdr := make([]byte, 5)
	binary.LittleEndian.PutUint32(hdr[0:4], IndexMagic)
	hdr[4] = IndexVersion
	if _, err := f.Write(hdr); err != nil {
		return err
	}

	// write symbol table
	// keep deterministic sorted mapping for symbol indices used by series
	sortedSyms := make([]string, len(symbols))
	copy(sortedSyms, symbols)
	sort.Strings(sortedSyms)

	symOff64, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := WriteSymbolTable(f, sortedSyms); err != nil {
		return err
	}

	// write series section and obtain series IDs
	seriesOff64, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	seriesIDs, err := WriteSeriesSection(f, series, seriesOff64)
	if err != nil {
		return err
	}

	// build postings map by scanning series label pairs and collecting series IDs
	postingsMap := make(map[[2]uint32][]uint64)
	for i, s := range series {
		id := seriesIDs[i]
		for _, p := range s.LabelPairs {
			key := [2]uint32{p[0], p[1]}
			postingsMap[key] = append(postingsMap[key], id)
		}
	}

	// write postings sections in deterministic order (by name, then value)
	type pv struct {
		key   [2]uint32
		name  string
		value string
	}
	pvs := make([]pv, 0, len(postingsMap))
	for k := range postingsMap {
		n := ""
		v := ""
		if int(k[0]) < len(sortedSyms) {
			n = sortedSyms[k[0]]
		}
		if int(k[1]) < len(sortedSyms) {
			v = sortedSyms[k[1]]
		}
		pvs = append(pvs, pv{key: k, name: n, value: v})
	}
	sort.Slice(pvs, func(i, j int) bool {
		if pvs[i].name == pvs[j].name {
			return pvs[i].value < pvs[j].value
		}
		return pvs[i].name < pvs[j].name
	})

	postingsStart64, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	// record offsets for postings offset table
	postingsOffsets := make(map[[2]uint32]uint64)
	for _, entry := range pvs {
		off, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		postingsOffsets[entry.key] = uint64(off)
		if err := WritePostingsSection(f, postingsMap[entry.key]); err != nil {
			return err
		}
	}

	// write postings offset table
	postingsOffTblOff64, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	// build list of entries in same deterministic order
	poEntries := make([]PostingsOffsetEntry, 0, len(pvs))
	for _, entry := range pvs {
		poEntries = append(poEntries, PostingsOffsetEntry{
			Name:   entry.name,
			Value:  entry.value,
			Offset: postingsOffsets[entry.key],
		})
	}
	if err := WritePostingsOffsetTable(f, poEntries); err != nil {
		return err
	}

	toc := &TOC{
		Symbols:             uint64(symOff64),
		Series:              uint64(seriesOff64),
		PostingsStart:       uint64(postingsStart64),
		PostingsOffsetTable: uint64(postingsOffTblOff64),
	}
	// write TOC at EOF
	if err := WriteTOC(f, toc); err != nil {
		return err
	}
	return nil
}

// ValidateHeader reads and validates header (magic + version) at the beginning of the file.
func ValidateHeader(f *os.File) error {
	hdr := make([]byte, 5)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.ReadFull(f, hdr); err != nil {
		return err
	}
	magic := binary.LittleEndian.Uint32(hdr[0:4])
	if magic != IndexMagic {
		return fmt.Errorf("bad magic: got %08x want %08x", magic, IndexMagic)
	}
	if hdr[4] != IndexVersion {
		return fmt.Errorf("unsupported index version: %d", hdr[4])
	}
	return nil
}

// ------------------------------------------------------------------
// Symbol table section
// Format: uint32 len-of-body, body, CRC32
// body: #symbols (uint32 LE) then sequence of entries: len(uvarint) + bytes
// ------------------------------------------------------------------

// WriteSymbolTable writes the symbol table at the current file position.
func WriteSymbolTable(w io.Writer, symbols []string) error {
	// Sort symbols lexicographically for deterministic on-disk order
	syms := make([]string, len(symbols))
	copy(syms, symbols)
	sort.Strings(syms)

	// build body
	var body bytes.Buffer
	cnt := uint32(len(syms))
	if err := binary.Write(&body, binary.LittleEndian, cnt); err != nil {
		return err
	}
	for _, s := range syms {
		// write uvarint length then bytes
		tmp := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(tmp, uint64(len(s)))
		if _, err := body.Write(tmp[:n]); err != nil {
			return err
		}
		if _, err := body.Write([]byte(s)); err != nil {
			return err
		}
	}

	// write length prefix (uint32) then body then CRC32
	bodyBytes := body.Bytes()
	if err := binary.Write(w, binary.LittleEndian, uint32(len(bodyBytes))); err != nil {
		return err
	}
	if _, err := w.Write(bodyBytes); err != nil {
		return err
	}
	crc := crc32.ChecksumIEEE(bodyBytes)
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	if _, err := w.Write(crcBuf); err != nil {
		return err
	}
	return nil
}

// ReadSymbolTable reads a symbol table located at offset in the given file.
func ReadSymbolTable(f *os.File, offset int64) ([]string, error) {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	var bodyLen uint32
	if err := binary.Read(f, binary.LittleEndian, &bodyLen); err != nil {
		return nil, err
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(f, body); err != nil {
		return nil, err
	}
	crcBuf := make([]byte, 4)
	if _, err := io.ReadFull(f, crcBuf); err != nil {
		return nil, err
	}
	got := binary.LittleEndian.Uint32(crcBuf)
	want := crc32.ChecksumIEEE(body)
	if got != want {
		return nil, fmt.Errorf("symbol table checksum mismatch")
	}

	r := bytes.NewReader(body)
	var cnt uint32
	if err := binary.Read(r, binary.LittleEndian, &cnt); err != nil {
		return nil, err
	}
	symbols := make([]string, 0, cnt)
	for i := uint32(0); i < cnt; i++ {
		l, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		bs := make([]byte, l)
		if _, err := io.ReadFull(r, bs); err != nil {
			return nil, err
		}
		symbols = append(symbols, string(bs))
	}
	return symbols, nil
}

// PostingsOffsetEntry represents a single postings offset table entry.
type PostingsOffsetEntry struct {
	Name   string
	Value  string
	Offset uint64
}

// WritePostingsSection writes a postings section containing a list of series IDs.
// The section body format: #entries(uint32 LE) followed by series refs (uint32 LE),
// and the writer stores the body length (uint32) and trailing CRC32 as for other sections.
func WritePostingsSection(w io.Writer, seriesIDs []uint64) error {
	var body bytes.Buffer
	cnt := uint32(len(seriesIDs))
	if err := binary.Write(&body, binary.LittleEndian, cnt); err != nil {
		return err
	}
	for _, id := range seriesIDs {
		// store series ref as uint32 (file format uses 4 bytes for refs)
		if err := binary.Write(&body, binary.LittleEndian, uint32(id)); err != nil {
			return err
		}
	}

	bodyBytes := body.Bytes()
	if err := binary.Write(w, binary.LittleEndian, uint32(len(bodyBytes))); err != nil {
		return err
	}
	if _, err := w.Write(bodyBytes); err != nil {
		return err
	}
	crc := crc32.ChecksumIEEE(bodyBytes)
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	if _, err := w.Write(crcBuf); err != nil {
		return err
	}
	return nil
}

// WritePostingsOffsetTable writes the postings offset table. The body contains
// #entries (uint32 LE) followed by entries: len(name) uvarint, name bytes,
// len(value) uvarint, value bytes, offset uvarint64.
func WritePostingsOffsetTable(w io.Writer, entries []PostingsOffsetEntry) error {
	var body bytes.Buffer
	cnt := uint32(len(entries))
	if err := binary.Write(&body, binary.LittleEndian, cnt); err != nil {
		return err
	}
	tmp := make([]byte, binary.MaxVarintLen64)
	for _, e := range entries {
		n := binary.PutUvarint(tmp, uint64(len(e.Name)))
		if _, err := body.Write(tmp[:n]); err != nil {
			return err
		}
		if _, err := body.Write([]byte(e.Name)); err != nil {
			return err
		}
		n = binary.PutUvarint(tmp, uint64(len(e.Value)))
		if _, err := body.Write(tmp[:n]); err != nil {
			return err
		}
		if _, err := body.Write([]byte(e.Value)); err != nil {
			return err
		}
		n = binary.PutUvarint(tmp, e.Offset)
		if _, err := body.Write(tmp[:n]); err != nil {
			return err
		}
	}

	bodyBytes := body.Bytes()
	if err := binary.Write(w, binary.LittleEndian, uint32(len(bodyBytes))); err != nil {
		return err
	}
	if _, err := w.Write(bodyBytes); err != nil {
		return err
	}
	crc := crc32.ChecksumIEEE(bodyBytes)
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	if _, err := w.Write(crcBuf); err != nil {
		return err
	}
	return nil
}

// ------------------------------------------------------------------
// Series section (minimal implementation)
// Format: uint32 len-of-body, body, CRC32
// body: series entries concatenated. Each series: labels count (uvarint64),
// then for each label pair: ref(name) uvarint32, ref(value) uvarint32,
// then chunks count (uvarint64) (we support zero chunks in this minimal impl).
// ------------------------------------------------------------------

// SeriesEntry is a minimal representation used for tests and tooling.
type SeriesEntry struct {
	LabelPairs [][2]uint32 // indices into symbol table
	Chunks     []ChunkMeta
}

// ChunkMeta holds minimal chunk metadata referenced from the series section.
type ChunkMeta struct {
	Mint    int64  // timestamp of first sample in chunk
	Maxt    int64  // timestamp of last sample in chunk
	DataRef uint64 // reference to chunk payload in chunk file
}

// WriteSeriesSection writes the series section at the current writer position and
// returns the computed series IDs. The caller must provide the absolute file
// offset where the section body length prefix will be written (sectionOffset).
// Series IDs are computed as absolute file offset of each series entry divided
// by 16 as per the on-disk format.
func WriteSeriesSection(w io.Writer, series []SeriesEntry, sectionOffset int64) ([]uint64, error) {
	var body bytes.Buffer
	var currOffset int64 = 0
	seriesIDs := make([]uint64, 0, len(series))
	for _, s := range series {
		// compute pad to align entry within section
		pad := (16 - (currOffset % 16)) % 16
		if pad > 0 {
			if _, err := body.Write(bytes.Repeat([]byte{0}, int(pad))); err != nil {
				return nil, err
			}
			currOffset += pad
		}

		// entry start absolute offset = sectionOffset + 4 (body len prefix) + currOffset
		entryAbsOff := sectionOffset + 4 + currOffset
		seriesID := uint64(entryAbsOff / 16)
		seriesIDs = append(seriesIDs, seriesID)

		// prepare entry buffer for a single series entry
		var entry bytes.Buffer
		tmp := make([]byte, binary.MaxVarintLen64)
		// labels count
		n := binary.PutUvarint(tmp, uint64(len(s.LabelPairs)))
		if _, err := entry.Write(tmp[:n]); err != nil {
			return nil, err
		}
		// label pairs
		for _, p := range s.LabelPairs {
			// write name ref and value ref as uvarint
			n = binary.PutUvarint(tmp, uint64(p[0]))
			if _, err := entry.Write(tmp[:n]); err != nil {
				return nil, err
			}
			n = binary.PutUvarint(tmp, uint64(p[1]))
			if _, err := entry.Write(tmp[:n]); err != nil {
				return nil, err
			}
		}
		// chunks count
		n = binary.PutUvarint(tmp, uint64(len(s.Chunks)))
		if _, err := entry.Write(tmp[:n]); err != nil {
			return nil, err
		}
		// encode chunks with delta scheme into entry
		if len(s.Chunks) > 0 {
			// first chunk: write mint (varint), maxt-mint (uvarint), ref (uvarint)
			first := s.Chunks[0]
			vb := make([]byte, binary.MaxVarintLen64)
			m := binary.PutVarint(vb, first.Mint)
			if _, err := entry.Write(vb[:m]); err != nil {
				return nil, err
			}
			// maxt - mint
			m2 := binary.PutUvarint(vb, uint64(first.Maxt-first.Mint))
			if _, err := entry.Write(vb[:m2]); err != nil {
				return nil, err
			}
			// ref
			m3 := binary.PutUvarint(vb, first.DataRef)
			if _, err := entry.Write(vb[:m3]); err != nil {
				return nil, err
			}
			prevMaxt := first.Maxt
			prevRef := int64(first.DataRef)
			for i := 1; i < len(s.Chunks); i++ {
				c := s.Chunks[i]
				// mint delta = c.Mint - prevMaxt (uvarint)
				d := uint64(c.Mint - prevMaxt)
				m := binary.PutUvarint(tmp, d)
				if _, err := entry.Write(tmp[:m]); err != nil {
					return nil, err
				}
				// maxt - mint
				m = binary.PutUvarint(tmp, uint64(c.Maxt-c.Mint))
				if _, err := entry.Write(tmp[:m]); err != nil {
					return nil, err
				}
				// ref delta (varint)
				vb2 := make([]byte, binary.MaxVarintLen64)
				rd := binary.PutVarint(vb2, int64(c.DataRef)-prevRef)
				if _, err := entry.Write(vb2[:rd]); err != nil {
					return nil, err
				}
				prevMaxt = c.Maxt
				prevRef = int64(c.DataRef)
			}
		}

		// write entry
		eb := entry.Bytes()
		if _, err := body.Write(eb); err != nil {
			return nil, err
		}
		currOffset += int64(len(eb))
	}

	bodyBytes := body.Bytes()
	if err := binary.Write(w, binary.LittleEndian, uint32(len(bodyBytes))); err != nil {
		return nil, err
	}
	if _, err := w.Write(bodyBytes); err != nil {
		return nil, err
	}
	crc := crc32.ChecksumIEEE(bodyBytes)
	crcBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(crcBuf, crc)
	if _, err := w.Write(crcBuf); err != nil {
		return nil, err
	}
	return seriesIDs, nil
}

// ReadSeriesSection reads series entries from offset in the file.
func ReadSeriesSection(f *os.File, offset int64) ([]SeriesEntry, error) {
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}
	var bodyLen uint32
	if err := binary.Read(f, binary.LittleEndian, &bodyLen); err != nil {
		return nil, err
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(f, body); err != nil {
		return nil, err
	}
	crcBuf := make([]byte, 4)
	if _, err := io.ReadFull(f, crcBuf); err != nil {
		return nil, err
	}
	got := binary.LittleEndian.Uint32(crcBuf)
	want := crc32.ChecksumIEEE(body)
	if got != want {
		return nil, fmt.Errorf("series section checksum mismatch")
	}

	var ret []SeriesEntry
	r := bytes.NewReader(body)
	for r.Len() > 0 {
		// skip padding to align to 16 bytes
		consumed := int64(len(body) - r.Len())
		pad := (16 - (consumed % 16)) % 16
		if pad > 0 {
			// consume pad bytes
			if _, err := r.Seek(pad, io.SeekCurrent); err != nil {
				return nil, err
			}
		}

		c, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		pairs := make([][2]uint32, 0, c)
		for i := uint64(0); i < c; i++ {
			n1, err := binary.ReadUvarint(r)
			if err != nil {
				return nil, err
			}
			n2, err := binary.ReadUvarint(r)
			if err != nil {
				return nil, err
			}
			pairs = append(pairs, [2]uint32{uint32(n1), uint32(n2)})
		}
		// read chunks count
		chunksCount, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		var chunks []ChunkMeta
		if chunksCount > 0 {
			// first chunk: mint (varint), maxt-mint (uvarint), ref (uvarint)
			mint, err := binary.ReadVarint(r)
			if err != nil {
				return nil, err
			}
			maxtDelta, err := binary.ReadUvarint(r)
			if err != nil {
				return nil, err
			}
			ref0, err := binary.ReadUvarint(r)
			if err != nil {
				return nil, err
			}
			cm := ChunkMeta{Mint: mint, Maxt: mint + int64(maxtDelta), DataRef: ref0}
			chunks = append(chunks, cm)
			prevMaxt := cm.Maxt
			prevRef := int64(cm.DataRef)
			for i := uint64(1); i < chunksCount; i++ {
				mintDelta, err := binary.ReadUvarint(r)
				if err != nil {
					return nil, err
				}
				maxtDelta, err := binary.ReadUvarint(r)
				if err != nil {
					return nil, err
				}
				refDelta, err := binary.ReadVarint(r)
				if err != nil {
					return nil, err
				}
				mint := prevMaxt + int64(mintDelta)
				maxt := mint + int64(maxtDelta)
				ref := uint64(int64(prevRef) + refDelta)
				chunks = append(chunks, ChunkMeta{Mint: mint, Maxt: maxt, DataRef: ref})
				prevMaxt = maxt
				prevRef = int64(ref)
			}
		}
		ret = append(ret, SeriesEntry{LabelPairs: pairs, Chunks: chunks})
	}
	return ret, nil
}
