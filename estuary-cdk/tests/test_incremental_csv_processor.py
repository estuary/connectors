import pytest
import asyncio
from typing import Any, AsyncGenerator
from pydantic import BaseModel

from estuary_cdk.incremental_csv_processor import (
    IncrementalCSVProcessor,
    CSVConfig,
    CSVProcessingError,
)

class BasicRecord(BaseModel):
    name: str | None = None
    age: str | None = None
    city: str | None = None


class LargeDataRecord(BaseModel):
    id: str
    name: str
    email: str
    age: str
    department: str
    salary: str


class TestIncrementalCSVProcessor:
    """Comprehensive test suite for IncrementalCSVProcessor."""

    @pytest.fixture
    def sample_csv_data(self) -> str:
        """Basic CSV data for testing."""
        return """name,age,city
John,25,New York
Jane,30,Los Angeles
Bob,35,Chicago"""

    @pytest.fixture
    def large_csv_data(self) -> str:
        """Larger CSV dataset for performance testing."""
        header = "id,name,email,age,department,salary\n"
        rows = []
        for i in range(1000):
            rows.append(f"{i},User{i},user{i}@example.com,{20+i%50},Dept{i%10},{30000+i*100}")
        return header + "\n".join(rows)

    @pytest.fixture
    def csv_with_quotes(self) -> str:
        """CSV data with quoted fields."""
        return '''name,age,city
"John Doe","A ""great"" product",29.99
"Jane Smith","Product with
newline",15.50
"Bob Johnson","Simple product",10.00'''

    def csv_with_special_chars(self, delimiter: str = '|') -> str:
        """CSV with special characters and custom delimiter."""
        return f"""name{delimiter}age{delimiter}city
John{delimiter}Product with, comma{delimiter}"Quoted notes"
Jane{delimiter}Another product{delimiter}Simple notes
Bob{delimiter}"Quoted product"{delimiter}Notes with {delimiter} delimiter"""

    async def create_byte_chunk_iterator(self, data: str, chunk_size: int = 10, encoding: str = 'utf-8') -> AsyncGenerator[bytes, None]:
        """Helper to create async byte chunk iterator."""
        data_bytes = data.encode(encoding)
        for i in range(0, len(data_bytes), chunk_size):
            chunk = data_bytes[i:i + chunk_size]
            yield chunk
            await asyncio.sleep(0.001)

    @pytest.mark.asyncio
    async def test_basic_csv_processing(self, sample_csv_data):
        """Test basic CSV processing functionality."""
        chunk_iterator = self.create_byte_chunk_iterator(sample_csv_data, chunk_size=10)
        processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord)

        rows: list[BasicRecord] = []
        async for row in processor:
            rows.append(row)

        assert len(rows) == 3
        assert rows[0].name == "John" and rows[0].age == "25" and rows[0].city == "New York"
        assert rows[1].name == "Jane" and rows[1].age == "30" and rows[1].city == "Los Angeles"
        assert rows[2].name == "Bob" and rows[2].age == "35" and rows[2].city == "Chicago"

    @pytest.mark.asyncio
    async def test_different_chunk_sizes(self, sample_csv_data):
        """Test with various chunk sizes."""
        chunk_sizes = [1, 5, 10, 50, 100, len(sample_csv_data)]
        
        for chunk_size in chunk_sizes:
            chunk_iterator = self.create_byte_chunk_iterator(sample_csv_data, chunk_size=chunk_size)
            processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord)

            rows: list[BasicRecord] = []
            async for row in processor:
                rows.append(row)

            assert len(rows) == 3, f"Failed with chunk_size={chunk_size}"
            assert rows[0].name == "John", f"Failed with chunk_size={chunk_size}"

    @pytest.mark.asyncio
    async def test_quoted_fields(self, csv_with_quotes):
        """Test CSV with quoted fields and embedded quotes."""
        chunk_iterator = self.create_byte_chunk_iterator(csv_with_quotes, chunk_size=20)
        processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord)

        rows = []
        async for row in processor:
            rows.append(row)

        assert len(rows) == 3
        assert rows[0].name == "John Doe"
        assert rows[0].age == 'A "great" product'
        assert "newline" in rows[1].age

    @pytest.mark.asyncio
    async def test_different_delimiters(self):
        """Test CSV with custom delimiters."""
        delimiters = ['|', '\t', ';']

        for delimiter in delimiters:
            csv_data = self.csv_with_special_chars(delimiter)
            config = CSVConfig(delimiter=delimiter)
            chunk_iterator = self.create_byte_chunk_iterator(csv_data, chunk_size=15)
            processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord, config)

            rows: list[BasicRecord] = []
            async for row in processor:
                rows.append(row)

            assert len(rows) == 3, f"Failed with delimiter: {repr(delimiter)}"
            assert rows[0].name == "John", f"Failed with delimiter: {repr(delimiter)}"
            assert rows[0].age == "Product with, comma", f"Failed with delimiter: {repr(delimiter)}"

    @pytest.mark.asyncio
    async def test_different_line_terminators(self):
        """Test different line terminator styles."""
        test_cases = [
            ("name,age\nJohn,25\nJane,30", '\n'),
            ("name,age\r\nJohn,25\r\nJane,30", '\r\n'),
            ("name,age\rJohn,25\rJane,30", '\r'),
        ]

        for csv_data, line_terminator in test_cases:
            config = CSVConfig(lineterminator=line_terminator)
            chunk_iterator = self.create_byte_chunk_iterator(csv_data, chunk_size=8)
            processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord, config)

            rows: list[BasicRecord] = []
            async for row in processor:
                rows.append(row)

            assert len(rows) == 2, f"Failed with line terminator: {repr(line_terminator)}"
            assert rows[0].name == "John"

    @pytest.mark.asyncio
    async def test_empty_csv(self):
        """Test handling of empty CSV data."""
        chunk_iterator = self.create_byte_chunk_iterator("", chunk_size=10)
        processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord)

        rows: list[BasicRecord] = []
        async for row in processor:
            rows.append(row)

        assert len(rows) == 0

    @pytest.mark.asyncio
    async def test_single_row(self):
        """Test CSV with single data row."""
        csv_data = "name,age\nJohn,25"
        chunk_iterator = self.create_byte_chunk_iterator(csv_data, chunk_size=5)
        processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord)

        rows: list[BasicRecord] = []
        async for row in processor:
            rows.append(row)

        assert len(rows) == 1
        assert rows[0].name == "John" and rows[0].age == "25"

    @pytest.mark.asyncio
    async def test_unicode_decode_error(self):
        """Test that invalid encoding raises UnicodeDecodeError."""
        # Create bytes that are invalid UTF-8.
        invalid_utf8 = b"name,age\ntest,\xff\xfe"  # Invalid UTF-8 sequence.

        async def invalid_byte_iterator():
            yield invalid_utf8

        processor = IncrementalCSVProcessor(invalid_byte_iterator(), BasicRecord)

        with pytest.raises(UnicodeDecodeError):
            async for _ in processor:
                pass

    @pytest.mark.asyncio
    async def test_malformed_csv_with_unmatched_quotes(self):
        """Test that malformed CSV with unmatched quotes raises an error."""
        # Use CSV with unmatched quotes which should fail.
        malformed_csv = b'name,age,city\nJohn,25,"New York\nJane,30,Los Angeles'

        async def malformed_iterator():
            yield malformed_csv

        processor = IncrementalCSVProcessor(malformed_iterator(), BasicRecord)

        # Should raise CSVProcessingError for malformed data.
        with pytest.raises(CSVProcessingError):
            async for _ in processor:
                pass

    @pytest.mark.asyncio
    async def test_unicode_content(self):
        """Test CSV with Unicode characters."""
        unicode_csv = b"""name,age,city
Jos\xc3\xa9,S\xc3\xa3o Paulo,Brasil
Fran\xc3\xa7ois,Paris,France
\xe5\xbc\xa0\xe4\xb8\x89,\xe5\x8c\x97\xe4\xba\xac,\xe4\xb8\xad\xe5\x9b\xbd"""

        async def unicode_iterator():
            yield unicode_csv

        processor = IncrementalCSVProcessor(unicode_iterator(), BasicRecord)

        rows: list[BasicRecord] = []
        async for row in processor:
            rows.append(row)

        assert len(rows) == 3
        assert rows[0].name == "José"
        assert rows[1].age == "Paris"  # age field contains city content
        assert rows[2].city == "中国"

    @pytest.mark.asyncio
    async def test_large_dataset_processing(self, large_csv_data):
        """Test processing large dataset."""
        chunk_iterator = self.create_byte_chunk_iterator(large_csv_data, chunk_size=100)
        processor = IncrementalCSVProcessor(chunk_iterator, LargeDataRecord)

        row_count = 0
        first_row = None
        last_row = None

        async for row in processor:
            if first_row is None:
                first_row = row
            last_row = row
            row_count += 1

        assert row_count == 1000
        assert first_row is not None and last_row is not None
        assert first_row.id == "0"
        assert last_row.id == "999"

    @pytest.mark.asyncio
    async def test_chunk_boundaries_split_records(self):
        """Test that records split across chunk boundaries are handled correctly."""
        csv_data = "name,age,city\nJohn,Very long description that will definitely be split across multiple small chunks,extra\nJane,Short desc,data"

        # Use very small chunks to force splitting within records.
        chunk_iterator = self.create_byte_chunk_iterator(csv_data, chunk_size=3)
        processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord)

        rows = []
        async for row in processor:
            rows.append(row)

        assert len(rows) == 2
        assert "Very long description" in rows[0].age
        assert rows[1].name == "Jane"

    @pytest.mark.asyncio
    async def test_different_encodings(self):
        """Test different character encodings."""
        csv_data = "name,age,city\nJosé,São Paulo,Brasil\nFrançois,Paris,France"

        encodings = ['utf-8', 'utf-16', 'latin1']
        for encoding in encodings:
            config = CSVConfig(encoding=encoding)
            chunk_iterator = self.create_byte_chunk_iterator(csv_data, chunk_size=10, encoding=encoding)
            processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord, config)

            rows: list[BasicRecord] = []
            async for row in processor:
                rows.append(row)

            assert len(rows) == 2, f"Failed with encoding: {encoding}"

    @pytest.mark.asyncio
    async def test_csv_with_empty_fields(self):
        """Test CSV with empty fields."""
        csv_data = "name,age,city\nJohn,,New York\n,25,\nJane,30,Los Angeles"

        chunk_iterator = self.create_byte_chunk_iterator(csv_data, chunk_size=12)
        processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord)

        rows: list[BasicRecord] = []
        async for row in processor:
            rows.append(row)

        assert len(rows) == 3
        assert rows[0].age == ""
        assert rows[1].name == ""
        assert rows[1].city == ""

    @pytest.mark.asyncio
    async def test_csv_with_trailing_newlines(self):
        """Test CSV with trailing newlines."""
        csv_data = "name,age\nJohn,25\nJane,30\n\n\n"

        chunk_iterator = self.create_byte_chunk_iterator(csv_data, chunk_size=10)
        processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord)

        rows: list[BasicRecord] = []
        async for row in processor:
            rows.append(row)

        assert len(rows) == 2  # Empty lines should be filtered out.

    @pytest.mark.asyncio
    async def test_csv_with_inconsistent_fields_is_allowed(self):
        """Test that CSV with inconsistent field counts is actually allowed by CSV parsers."""
        # This is actually valid CSV - parsers fill missing fields with None.
        csv_with_varying_fields = b'name,age,city\nJohn,25,New York\nJane\nBob,35\n,,'

        async def csv_iterator():
            yield csv_with_varying_fields

        processor = IncrementalCSVProcessor(csv_iterator(), BasicRecord)

        rows: list[BasicRecord] = []
        async for row in processor:
            rows.append(row)

        # Should get all rows, including ones with missing fields and empty rows.
        assert len(rows) == 4
        assert rows[0].name == "John" and rows[0].age == "25" and rows[0].city == "New York"
        assert rows[1].name == "Jane" and rows[1].age is None and rows[1].city is None
        assert rows[2].name == "Bob" and rows[2].age == "35" and rows[2].city is None
        assert rows[3].name == "" and rows[3].age == "" and rows[3].city == ""

    @pytest.mark.asyncio
    async def test_headers_only(self):
        """Test CSV with only headers."""
        csv_data = b"name,age,city"

        async def header_iterator():
            yield csv_data

        processor = IncrementalCSVProcessor(header_iterator(), BasicRecord)

        rows: list[BasicRecord] = []
        async for row in processor:
            rows.append(row)

        # Should return no rows (only headers).
        assert len(rows) == 0

    @pytest.mark.asyncio
    async def test_very_long_field(self):
        """Test CSV with very long field values."""
        long_description = "A" * 10000  # 10KB field
        csv_data = f"name,age,city\nJohn,{long_description},extra\nJane,Short,data"

        chunk_iterator = self.create_byte_chunk_iterator(csv_data, chunk_size=100)
        processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord)

        rows = []
        async for row in processor:
            rows.append(row)

        assert len(rows) == 2
        assert len(rows[0].age) == 10000
        assert rows[1].age == "Short"

    @pytest.mark.asyncio
    async def test_concurrent_processing(self, sample_csv_data):
        """Test that multiple processors can work concurrently."""
        async def process_data(data, chunk_size):
            chunk_iterator = self.create_byte_chunk_iterator(data, chunk_size=chunk_size)
            processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord)

            rows: list[BasicRecord] = []
            async for row in processor:
                rows.append(row)
            return rows

        # Process same data with different chunk sizes concurrently.
        tasks = [
            process_data(sample_csv_data, 5),
            process_data(sample_csv_data, 10),
            process_data(sample_csv_data, 20),
        ]

        results = await asyncio.gather(*tasks)

        # All should produce same results.
        for result in results:
            assert len(result) == 3
            assert result[0].name == "John"

    @pytest.mark.asyncio
    async def test_explicit_fieldnames(self):
        """Test CSV processing with explicit field names (no header row)."""
        csv_data_no_headers = """John,25,New York
Jane,30,Los Angeles
Bob,35,Chicago"""

        chunk_iterator = self.create_byte_chunk_iterator(csv_data_no_headers, chunk_size=10)
        fieldnames = ['name', 'age', 'city']
        processor = IncrementalCSVProcessor(chunk_iterator, BasicRecord, fieldnames=fieldnames)

        rows: list[BasicRecord] = []
        async for row in processor:
            rows.append(row)

        assert len(rows) == 3
        assert rows[0].name == "John" and rows[0].age == "25" and rows[0].city == "New York"
        assert rows[1].name == "Jane" and rows[1].age == "30" and rows[1].city == "Los Angeles"
        assert rows[2].name == "Bob" and rows[2].age == "35" and rows[2].city == "Chicago"

    @pytest.mark.asyncio
    async def test_basic_dict_processing(self):
        """Test basic CSV processing without a Pydantic model yields dicts."""
        csv_data = """name,age,city
John,25,New York
Jane,30,Los Angeles"""

        chunk_iterator = self.create_byte_chunk_iterator(csv_data, chunk_size=10)
        processor = IncrementalCSVProcessor(chunk_iterator)

        rows: list[dict[str, Any]] = []
        async for row in processor:
            rows.append(row)

        assert len(rows) == 2
        assert isinstance(rows[0], dict)
        assert rows[0] == {"name": "John", "age": "25", "city": "New York"}
        assert rows[1] == {"name": "Jane", "age": "30", "city": "Los Angeles"}

    @pytest.mark.asyncio
    async def test_dict_mode_preserves_all_string_values(self):
        """Test that dict mode preserves all values as strings without validation."""
        csv_data = """name,age,active,score
John,25,true,99.5
Jane,invalid_age,false,N/A"""

        chunk_iterator = self.create_byte_chunk_iterator(csv_data, chunk_size=10)
        processor = IncrementalCSVProcessor(chunk_iterator)

        rows: list[dict[str, Any]] = []
        async for row in processor:
            rows.append(row)

        assert len(rows) == 2
        # All values should be strings - no type conversion
        assert rows[0] == {"name": "John", "age": "25", "active": "true", "score": "99.5"}
        assert rows[1] == {"name": "Jane", "age": "invalid_age", "active": "false", "score": "N/A"}
