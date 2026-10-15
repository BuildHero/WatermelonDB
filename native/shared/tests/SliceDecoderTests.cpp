#define private public
#include "../SliceDecoder.h"
#undef private

#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

namespace {

static int gFailures = 0;

void expectTrue(bool value, const char* message) {
    if (!value) {
        std::cerr << "FAIL: " << message << "\n";
        gFailures++;
    }
}

void appendVarint(std::vector<uint8_t>& out, uint64_t value) {
    while (value >= 0x80) {
        out.push_back(static_cast<uint8_t>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    out.push_back(static_cast<uint8_t>(value));
}

void appendString(std::vector<uint8_t>& out, const std::string& value) {
    appendVarint(out, static_cast<uint64_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
}

void appendTextField(std::vector<uint8_t>& out, const std::string& value) {
    appendVarint(out, static_cast<uint64_t>(value.size()));
    out.insert(out.end(), value.begin(), value.end());
    out.push_back(static_cast<uint8_t>(watermelondb::TypeTag::TEXT));
}

void appendZeroSizeField(std::vector<uint8_t>& out, watermelondb::TypeTag tag) {
    appendVarint(out, 0);
    out.push_back(static_cast<uint8_t>(tag));
}

void loadDecoder(watermelondb::SliceDecoder& decoder, const std::vector<uint8_t>& data, bool streamEnded) {
    decoder.streamInitialized_ = true;
    decoder.streamEnded_ = streamEnded;
    decoder.decompressedBuffer_ = data;
    decoder.decompressedSize_ = data.size();
    decoder.currentOffset_ = 0;
}

void test_varint_and_string_decode() {
    std::vector<uint8_t> buf;
    appendVarint(buf, 300);
    auto res = watermelondb::VarintDecoder::decodeVarint(buf.data(), buf.size(), 0);
    expectTrue(res.success && res.value == 300, "varint decode should work");

    std::vector<uint8_t> buf2;
    appendString(buf2, "hello");
    auto res2 = watermelondb::VarintDecoder::decodeString(buf2.data(), buf2.size(), 0);
    expectTrue(res2.success && res2.value == "hello", "string decode should work");
}

void test_parse_header_table_row() {
    std::vector<uint8_t> data;
    appendString(data, "slice1"); // sliceId
    appendVarint(data, 1); // version
    appendString(data, "high"); // priority
    appendVarint(data, 123); // timestamp
    appendVarint(data, 1); // numberOfTables

    appendString(data, "tasks");
    appendVarint(data, 2); // columns
    appendString(data, "id");
    appendString(data, "name");

    appendTextField(data, "t1");
    appendTextField(data, "Alpha");
    data.push_back(watermelondb::END_OF_TABLE_DELIMITER);

    watermelondb::SliceDecoder decoder;
    decoder.streamInitialized_ = true;
    decoder.streamEnded_ = true;
    decoder.decompressedBuffer_ = data;
    decoder.decompressedSize_ = data.size();
    decoder.currentOffset_ = 0;

    watermelondb::SliceHeader header;
    expectTrue(decoder.parseSliceHeader(header) == watermelondb::ParseStatus::Ok, "parseSliceHeader ok");

    watermelondb::TableHeader table;
    expectTrue(decoder.parseTableHeader(table) == watermelondb::ParseStatus::Ok, "parseTableHeader ok");
    expectTrue(table.tableName == "tasks", "table name parsed");
    expectTrue(table.columns.size() == 2, "columns parsed");

    watermelondb::Row row;
    expectTrue(decoder.parseRow(table.columns, row) == watermelondb::ParseStatus::Ok, "parseRow ok");
    expectTrue(row["id"].textValue == "t1", "row id parsed");
    expectTrue(row["name"].textValue == "Alpha", "row name parsed");

    std::vector<watermelondb::FieldValue> rowValues;
    expectTrue(decoder.parseRowValues(table.columns, rowValues) == watermelondb::ParseStatus::EndOfTable,
               "end of table detected");
    expectTrue(decoder.parseTableHeader(table) == watermelondb::ParseStatus::EndOfStream,
               "end of stream detected");
}

void test_invalid_column_count() {
    std::vector<uint8_t> data;
    appendString(data, "tasks");
    appendVarint(data, 201); // invalid

    watermelondb::SliceDecoder decoder;
    decoder.streamInitialized_ = true;
    decoder.streamEnded_ = true;
    decoder.decompressedBuffer_ = data;
    decoder.decompressedSize_ = data.size();
    decoder.currentOffset_ = 0;
    decoder.expectingTableHeader_ = true;
    decoder.expectedTables_ = 1;
    decoder.tablesParsed_ = 0;

    watermelondb::TableHeader table;
    expectTrue(decoder.parseTableHeader(table) == watermelondb::ParseStatus::Error,
               "invalid column count should error");
}

void test_invalid_field_size() {
    std::vector<uint8_t> data;
    appendVarint(data, watermelondb::MAX_FIELD_SIZE + 1);
    data.push_back(static_cast<uint8_t>(watermelondb::TypeTag::TEXT));

    watermelondb::SliceDecoder decoder;
    decoder.streamInitialized_ = true;
    decoder.streamEnded_ = true;
    decoder.decompressedBuffer_ = data;
    decoder.decompressedSize_ = data.size();
    decoder.currentOffset_ = 0;

    std::vector<std::string> columns = {"col"};
    std::vector<watermelondb::FieldValue> values;
    expectTrue(decoder.parseRowValues(columns, values) == watermelondb::ParseStatus::Error,
               "field size exceeding max should error");
}

void test_zero_size_fields_follow_type_tag() {
    using watermelondb::FieldValue;
    using watermelondb::TypeTag;

    std::vector<uint8_t> data;
    appendZeroSizeField(data, TypeTag::TEXT);
    appendZeroSizeField(data, TypeTag::NULL_TYPE);
    appendZeroSizeField(data, TypeTag::BLOB);
    appendZeroSizeField(data, TypeTag::INT);
    appendTextField(data, "x");
    appendZeroSizeField(data, TypeTag::NULL_TYPE);
    appendZeroSizeField(data, TypeTag::TEXT);
    appendZeroSizeField(data, TypeTag::REAL);
    appendTextField(data, "");
    appendTextField(data, "y");
    data.push_back(watermelondb::END_OF_TABLE_DELIMITER);

    watermelondb::SliceDecoder decoder;
    loadDecoder(decoder, data, true);

    std::vector<std::string> columns = {"empty_text", "null_text", "empty_blob", "zero_int", "text"};
    std::vector<FieldValue> values;
    expectTrue(decoder.parseRowValues(columns, values) == watermelondb::ParseStatus::Ok, "first row parses");
    expectTrue(values[0].type == FieldValue::Type::TEXT_VALUE && values[0].textValue.empty(),
               "zero-size TEXT decodes as empty string");
    expectTrue(values[1].type == FieldValue::Type::NULL_VALUE, "zero-size NULL decodes as NULL");
    expectTrue(values[2].type == FieldValue::Type::BLOB_VALUE && values[2].blobValue.empty(),
               "zero-size BLOB decodes as empty blob");
    expectTrue(values[3].type == FieldValue::Type::NULL_VALUE, "zero-size INT still decodes as NULL");
    expectTrue(values[4].type == FieldValue::Type::TEXT_VALUE && values[4].textValue == "x",
               "non-empty TEXT unchanged");

    expectTrue(decoder.parseRowValues(columns, values) == watermelondb::ParseStatus::Ok, "second row parses");
    expectTrue(values[0].type == FieldValue::Type::NULL_VALUE, "mixed column: NULL after empty string");
    expectTrue(values[1].type == FieldValue::Type::TEXT_VALUE && values[1].textValue.empty(),
               "mixed column: empty string after NULL");
    expectTrue(values[2].type == FieldValue::Type::NULL_VALUE, "zero-size REAL still decodes as NULL");
    expectTrue(values[3].type == FieldValue::Type::TEXT_VALUE && values[3].textValue.empty(),
               "appendTextField(\"\") decodes as empty string");
    expectTrue(values[4].type == FieldValue::Type::TEXT_VALUE && values[4].textValue == "y",
               "second row text unchanged");

    expectTrue(decoder.parseRowValues(columns, values) == watermelondb::ParseStatus::EndOfTable,
               "end of table after mixed rows");
}

void test_zero_size_field_waits_for_type_tag() {
    std::vector<uint8_t> data;
    appendVarint(data, 0);

    watermelondb::SliceDecoder decoder;
    loadDecoder(decoder, data, false);

    std::vector<std::string> columns = {"col"};
    std::vector<watermelondb::FieldValue> values;
    expectTrue(decoder.parseRowValues(columns, values) == watermelondb::ParseStatus::NeedMoreData,
               "zero-size field without its tag needs more data");
    expectTrue(decoder.currentOffset_ == 0, "offset not advanced while waiting for tag");

    decoder.decompressedBuffer_.push_back(static_cast<uint8_t>(watermelondb::TypeTag::TEXT));
    decoder.decompressedSize_ = decoder.decompressedBuffer_.size();
    expectTrue(decoder.parseRowValues(columns, values) == watermelondb::ParseStatus::Ok,
               "zero-size field parses once its tag arrives");
    expectTrue(values[0].type == watermelondb::FieldValue::Type::TEXT_VALUE && values[0].textValue.empty(),
               "late-arriving TEXT tag decodes as empty string");
}

} // namespace

int main() {
    test_varint_and_string_decode();
    test_parse_header_table_row();
    test_invalid_column_count();
    test_invalid_field_size();
    test_zero_size_fields_follow_type_tag();
    test_zero_size_field_waits_for_type_tag();

    if (gFailures > 0) {
        std::cerr << gFailures << " test(s) failed\n";
        return 1;
    }
    std::cout << "All SliceDecoder tests passed\n";
    return 0;
}
