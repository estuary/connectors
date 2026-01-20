// Package spatial parses MySQL's internal geometry representation and produces
// WKT (Well-Known Text) output.
//
// # MySQL Internal Geometry Format
//
// MySQL stores geometry values in a custom binary format that consists of:
//
//	[4 bytes: SRID] [WKB data]
//
// The SRID (Spatial Reference System Identifier) is a little-endian uint32 that
// identifies the coordinate system. Common values include 0 (no SRID) and 4326
// (WGS 84 geographic coordinates). This package discards the SRID, matching the
// behavior of MySQL's ST_AsText() function.
//
// Note: MySQL's format is NOT the same as EWKB (PostGIS Extended WKB). In EWKB,
// the SRID is encoded in the type field with a flag bit; in MySQL, it's a fixed
// 4-byte prefix before standard WKB.
//
// # Well-Known Binary (WKB) Format
//
// WKB is an OGC standard binary format for geometry. Each geometry starts with
// a 5-byte header:
//
//	[1 byte: byte order] [4 bytes: geometry type]
//
// Byte order: 0x00 = big-endian, 0x01 = little-endian.
// MySQL always uses little-endian (0x01) for internal storage.
//
// # Geometry Type Codes
//
//	1 = Point
//	2 = LineString
//	3 = Polygon
//	4 = MultiPoint
//	5 = MultiLineString
//	6 = MultiPolygon
//	7 = GeometryCollection
//
// # Type-Specific Binary Structures
//
// All multi-byte integers are little-endian uint32. All coordinates are
// little-endian IEEE 754 float64 (8 bytes each).
//
// Point:
//
//	[8 bytes: X] [8 bytes: Y]
//
// LineString:
//
//	[4 bytes: numPoints]
//	[numPoints × 16 bytes: X,Y coordinate pairs]
//
// Polygon:
//
//	[4 bytes: numRings]
//	For each ring:
//	  [4 bytes: numPoints]
//	  [numPoints × 16 bytes: X,Y coordinate pairs]
//
// The first ring is the exterior boundary; subsequent rings are holes.
// Each ring is closed (first point equals last point).
//
// MultiPoint:
//
//	[4 bytes: numPoints]
//	For each point:
//	  [5 bytes: WKB header (byte order + type)]
//	  [16 bytes: X,Y coordinates]
//
// MultiLineString:
//
//	[4 bytes: numLineStrings]
//	For each linestring:
//	  [5 bytes: WKB header]
//	  [LineString data as above]
//
// MultiPolygon:
//
//	[4 bytes: numPolygons]
//	For each polygon:
//	  [5 bytes: WKB header]
//	  [Polygon data as above]
//
// GeometryCollection:
//
//	[4 bytes: numGeometries]
//	For each geometry:
//	  [Complete WKB including header]
//
// GeometryCollection can contain any geometry type, including nested collections.
//
// # Well-Known Text (WKT) Output Format
//
// WKT is a human-readable text format for geometry defined by the OGC Simple
// Feature Access specification. Coordinates within a point are space-separated;
// points are comma-separated. Parentheses group structural elements.
//
// Examples:
//
//	POINT(1 2)
//	LINESTRING(0 0,1 1,2 2)
//	POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,8 2,8 8,2 8,2 2))
//	MULTIPOINT((0 0),(1 1),(2 2))
//	MULTILINESTRING((0 0,1 1),(2 2,3 3))
//	MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))
//	GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(1 1,2 2))
//	GEOMETRYCOLLECTION EMPTY
//
// Key formatting rules:
//   - MULTIPOINT uses parentheses around each point's coordinates per OGC spec
//   - Empty collections use "GEOMETRYCOLLECTION EMPTY" (with space, not parens)
//   - Coordinates use minimal precision that round-trips the float64 value
//   - No spaces after commas, no spaces inside parentheses
//
// # Coordinate Precision
//
// Coordinates are formatted using Go's strconv.FormatFloat with 'g' format and
// precision -1, which produces the shortest representation that round-trips.
// This matches MySQL's behavior of using dtoa with maximum precision while
// stripping trailing zeros.
package spatial

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Geometry type codes from WKB specification.
const (
	wkbPoint              = 1
	wkbLineString         = 2
	wkbPolygon            = 3
	wkbMultiPoint         = 4
	wkbMultiLineString    = 5
	wkbMultiPolygon       = 6
	wkbGeometryCollection = 7
)

// Size constants.
const (
	sridSize      = 4 // SRID prefix size
	wkbHeaderSize = 5 // 1 byte order + 4 byte type
)

// ParseToWKT converts MySQL internal geometry bytes to WKT string.
// The SRID prefix is read but not included in output, matching MySQL's
// ST_AsText() behavior. For geographic SRIDs (like 4326), coordinates are
// swapped to match the SRS-defined axis order (lat/lon instead of lon/lat).
func ParseToWKT(data []byte) (string, error) {
	if len(data) < sridSize+wkbHeaderSize {
		return "", fmt.Errorf("geometry data too short: need at least %d bytes, got %d",
			sridSize+wkbHeaderSize, len(data))
	}

	// Read SRID to determine if axis swap is needed
	srid := binary.LittleEndian.Uint32(data[0:4])

	// Start parsing WKB after SRID prefix
	r := &wkbReader{
		data:     data,
		pos:      sridSize,
		swapAxes: isGeographicSRID(srid),
	}
	return r.readGeometry()
}

// isGeographicSRID returns true if the SRID represents a geographic (lat/lon)
// coordinate system where MySQL's ST_AsText() swaps axes.
//
// MySQL 8.0.12+ respects the axis order defined by geographic SRIDs. For SRID
// 4326 (WGS 84), the standard axis order is latitude/longitude, so ST_AsText()
// outputs Y before X.
//
// This function currently handles SRID 4326 explicitly. For a comprehensive
// solution, we'd need to query MySQL's information_schema.ST_SPATIAL_REFERENCE_SYSTEMS
// or maintain a lookup table of geographic SRIDs.
func isGeographicSRID(srid uint32) bool {
	// SRID 4326 is WGS 84, the most common geographic coordinate system
	return srid == 4326
}

// wkbReader provides sequential reading of WKB binary data.
type wkbReader struct {
	data     []byte
	pos      int
	swapAxes bool // true for geographic SRIDs where MySQL swaps X/Y in output
}

// remaining returns the number of unread bytes.
func (r *wkbReader) remaining() int {
	return len(r.data) - r.pos
}

// readByte reads a single byte.
func (r *wkbReader) readByte() (byte, error) {
	if r.remaining() < 1 {
		return 0, fmt.Errorf("unexpected end of data reading byte at position %d", r.pos)
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

// readUint32 reads a little-endian uint32.
func (r *wkbReader) readUint32() (uint32, error) {
	if r.remaining() < 4 {
		return 0, fmt.Errorf("unexpected end of data reading uint32 at position %d", r.pos)
	}
	v := binary.LittleEndian.Uint32(r.data[r.pos:])
	r.pos += 4
	return v, nil
}

// readFloat64 reads a little-endian IEEE 754 float64.
func (r *wkbReader) readFloat64() (float64, error) {
	if r.remaining() < 8 {
		return 0, fmt.Errorf("unexpected end of data reading float64 at position %d", r.pos)
	}
	bits := binary.LittleEndian.Uint64(r.data[r.pos:])
	r.pos += 8
	return math.Float64frombits(bits), nil
}

// readGeometry reads a complete geometry (header + data) and returns WKT.
func (r *wkbReader) readGeometry() (string, error) {
	// Read WKB header
	byteOrder, err := r.readByte()
	if err != nil {
		return "", fmt.Errorf("reading byte order: %w", err)
	}
	if byteOrder != 0x01 {
		return "", fmt.Errorf("unsupported byte order %#x (expected 0x01 little-endian)", byteOrder)
	}

	geomType, err := r.readUint32()
	if err != nil {
		return "", fmt.Errorf("reading geometry type: %w", err)
	}

	switch geomType {
	case wkbPoint:
		return r.readPoint()
	case wkbLineString:
		return r.readLineString()
	case wkbPolygon:
		return r.readPolygon()
	case wkbMultiPoint:
		return r.readMultiPoint()
	case wkbMultiLineString:
		return r.readMultiLineString()
	case wkbMultiPolygon:
		return r.readMultiPolygon()
	case wkbGeometryCollection:
		return r.readGeometryCollection()
	default:
		return "", fmt.Errorf("unsupported geometry type %d", geomType)
	}
}

// formatCoord formats a float64 coordinate value to match MySQL's output.
//
// MySQL uses my_gcvt() which produces the shortest representation that
// round-trips, preferring fixed-point notation when practical. It only uses
// scientific notation for extremely large/small values.
//
// Go's 'g' format switches to scientific notation more aggressively than MySQL.
// We use 'f' format (fixed-point) with -1 precision for shortest representation,
// which matches MySQL's preference for fixed-point. For extreme values that
// would produce excessively long strings, we fall back to scientific notation.
func formatCoord(v float64) string {
	// For extreme values, use scientific notation
	abs := v
	if abs < 0 {
		abs = -abs
	}
	// Use scientific notation for values >= 1e15 or < 1e-14 (but not zero)
	// These thresholds match MySQL's behavior of preferring fixed-point
	if abs != 0 && (abs >= 1e15 || abs < 1e-14) {
		s := strconv.FormatFloat(v, 'g', -1, 64)
		// Remove '+' from exponents: "1e+308" -> "1e308"
		return strings.ReplaceAll(s, "e+", "e")
	}
	// Use fixed-point notation with minimal precision
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// readPoint reads Point data and returns WKT.
func (r *wkbReader) readPoint() (string, error) {
	coords, err := r.readPointCoords()
	if err != nil {
		return "", fmt.Errorf("reading point: %w", err)
	}
	return "POINT(" + coords + ")", nil
}

// readPointCoords reads X,Y coordinates and returns coordinate string.
// For geographic SRIDs with swapAxes=true, outputs "Y X" to match MySQL's
// ST_AsText() which respects the SRS axis order (lat/lon for WGS 84).
func (r *wkbReader) readPointCoords() (string, error) {
	x, err := r.readFloat64()
	if err != nil {
		return "", fmt.Errorf("reading X: %w", err)
	}
	y, err := r.readFloat64()
	if err != nil {
		return "", fmt.Errorf("reading Y: %w", err)
	}
	if r.swapAxes {
		return formatCoord(y) + " " + formatCoord(x), nil
	}
	return formatCoord(x) + " " + formatCoord(y), nil
}

// readPointList reads a sequence of points and returns comma-separated coords.
func (r *wkbReader) readPointList(numPoints uint32) (string, error) {
	points := make([]string, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		coords, err := r.readPointCoords()
		if err != nil {
			return "", fmt.Errorf("reading point %d: %w", i, err)
		}
		points[i] = coords
	}
	return strings.Join(points, ","), nil
}

// readLineString reads LineString data and returns WKT.
func (r *wkbReader) readLineString() (string, error) {
	numPoints, err := r.readUint32()
	if err != nil {
		return "", fmt.Errorf("reading linestring point count: %w", err)
	}
	coords, err := r.readPointList(numPoints)
	if err != nil {
		return "", fmt.Errorf("reading linestring points: %w", err)
	}
	return fmt.Sprintf("LINESTRING(%s)", coords), nil
}

// readRing reads a polygon ring (point count + points) and returns coords string.
func (r *wkbReader) readRing() (string, error) {
	numPoints, err := r.readUint32()
	if err != nil {
		return "", fmt.Errorf("reading ring point count: %w", err)
	}
	return r.readPointList(numPoints)
}

// readPolygon reads Polygon data and returns WKT.
func (r *wkbReader) readPolygon() (string, error) {
	numRings, err := r.readUint32()
	if err != nil {
		return "", fmt.Errorf("reading polygon ring count: %w", err)
	}
	rings := make([]string, numRings)
	for i := uint32(0); i < numRings; i++ {
		coords, err := r.readRing()
		if err != nil {
			return "", fmt.Errorf("reading ring %d: %w", i, err)
		}
		rings[i] = "(" + coords + ")"
	}
	return fmt.Sprintf("POLYGON(%s)", strings.Join(rings, ",")), nil
}

// readMultiPoint reads MultiPoint data and returns WKT.
func (r *wkbReader) readMultiPoint() (string, error) {
	numPoints, err := r.readUint32()
	if err != nil {
		return "", fmt.Errorf("reading multipoint count: %w", err)
	}
	points := make([]string, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		// Each point has its own WKB header
		byteOrder, err := r.readByte()
		if err != nil {
			return "", fmt.Errorf("reading point %d byte order: %w", i, err)
		}
		if byteOrder != 0x01 {
			return "", fmt.Errorf("point %d: unsupported byte order %#x", i, byteOrder)
		}
		geomType, err := r.readUint32()
		if err != nil {
			return "", fmt.Errorf("reading point %d type: %w", i, err)
		}
		if geomType != wkbPoint {
			return "", fmt.Errorf("point %d: expected type 1 (Point), got %d", i, geomType)
		}
		coords, err := r.readPointCoords()
		if err != nil {
			return "", fmt.Errorf("reading point %d coords: %w", i, err)
		}
		// MULTIPOINT uses parentheses around each point's coordinates per OGC spec
		points[i] = "(" + coords + ")"
	}
	return fmt.Sprintf("MULTIPOINT(%s)", strings.Join(points, ",")), nil
}

// readLineStringData reads LineString data (without type prefix) and returns coords.
func (r *wkbReader) readLineStringData() (string, error) {
	numPoints, err := r.readUint32()
	if err != nil {
		return "", fmt.Errorf("reading point count: %w", err)
	}
	return r.readPointList(numPoints)
}

// readMultiLineString reads MultiLineString data and returns WKT.
func (r *wkbReader) readMultiLineString() (string, error) {
	numLineStrings, err := r.readUint32()
	if err != nil {
		return "", fmt.Errorf("reading multilinestring count: %w", err)
	}
	linestrings := make([]string, numLineStrings)
	for i := uint32(0); i < numLineStrings; i++ {
		// Each linestring has its own WKB header
		byteOrder, err := r.readByte()
		if err != nil {
			return "", fmt.Errorf("reading linestring %d byte order: %w", i, err)
		}
		if byteOrder != 0x01 {
			return "", fmt.Errorf("linestring %d: unsupported byte order %#x", i, byteOrder)
		}
		geomType, err := r.readUint32()
		if err != nil {
			return "", fmt.Errorf("reading linestring %d type: %w", i, err)
		}
		if geomType != wkbLineString {
			return "", fmt.Errorf("linestring %d: expected type 2 (LineString), got %d", i, geomType)
		}
		coords, err := r.readLineStringData()
		if err != nil {
			return "", fmt.Errorf("reading linestring %d data: %w", i, err)
		}
		linestrings[i] = "(" + coords + ")"
	}
	return fmt.Sprintf("MULTILINESTRING(%s)", strings.Join(linestrings, ",")), nil
}

// readPolygonData reads Polygon data (without type prefix) and returns rings string.
func (r *wkbReader) readPolygonData() (string, error) {
	numRings, err := r.readUint32()
	if err != nil {
		return "", fmt.Errorf("reading ring count: %w", err)
	}
	rings := make([]string, numRings)
	for i := uint32(0); i < numRings; i++ {
		coords, err := r.readRing()
		if err != nil {
			return "", fmt.Errorf("reading ring %d: %w", i, err)
		}
		rings[i] = "(" + coords + ")"
	}
	return strings.Join(rings, ","), nil
}

// readMultiPolygon reads MultiPolygon data and returns WKT.
func (r *wkbReader) readMultiPolygon() (string, error) {
	numPolygons, err := r.readUint32()
	if err != nil {
		return "", fmt.Errorf("reading multipolygon count: %w", err)
	}
	polygons := make([]string, numPolygons)
	for i := uint32(0); i < numPolygons; i++ {
		// Each polygon has its own WKB header
		byteOrder, err := r.readByte()
		if err != nil {
			return "", fmt.Errorf("reading polygon %d byte order: %w", i, err)
		}
		if byteOrder != 0x01 {
			return "", fmt.Errorf("polygon %d: unsupported byte order %#x", i, byteOrder)
		}
		geomType, err := r.readUint32()
		if err != nil {
			return "", fmt.Errorf("reading polygon %d type: %w", i, err)
		}
		if geomType != wkbPolygon {
			return "", fmt.Errorf("polygon %d: expected type 3 (Polygon), got %d", i, geomType)
		}
		rings, err := r.readPolygonData()
		if err != nil {
			return "", fmt.Errorf("reading polygon %d data: %w", i, err)
		}
		polygons[i] = "(" + rings + ")"
	}
	return fmt.Sprintf("MULTIPOLYGON(%s)", strings.Join(polygons, ",")), nil
}

// readGeometryCollection reads GeometryCollection data and returns WKT.
func (r *wkbReader) readGeometryCollection() (string, error) {
	numGeometries, err := r.readUint32()
	if err != nil {
		return "", fmt.Errorf("reading geometrycollection count: %w", err)
	}

	// Special case: empty collection uses "GEOMETRYCOLLECTION EMPTY"
	if numGeometries == 0 {
		return "GEOMETRYCOLLECTION EMPTY", nil
	}

	geometries := make([]string, numGeometries)
	for i := uint32(0); i < numGeometries; i++ {
		geom, err := r.readGeometry()
		if err != nil {
			return "", fmt.Errorf("reading geometry %d: %w", i, err)
		}
		geometries[i] = geom
	}
	return fmt.Sprintf("GEOMETRYCOLLECTION(%s)", strings.Join(geometries, ",")), nil
}
