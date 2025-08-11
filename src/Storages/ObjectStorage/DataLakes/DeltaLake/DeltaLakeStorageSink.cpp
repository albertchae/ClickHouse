#include "DeltaLakeStorageSink.h"
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadataDeltaKernel.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelUtils.h>
#include <Common/logger_useful.h>
#include <arrow/table.h>
#include <arrow/c/bridge.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_EXCEPTION;
}

DeltaLakeStorageSink::DeltaLakeStorageSink(
    const DeltaLakeMetadataDeltaKernel & metadata,
    SharedHeader sample_block_,
    const FormatSettings & format_settings_)
    : SinkToStorage(sample_block_)
    , kernel_helper(metadata.getKernelHelper())
    , log(getLogger("DeltaLakeStorageSink"))
    , format_settings(format_settings_)
{
    auto * engine_builder = kernel_helper->createBuilder();
    engine = DeltaLake::KernelUtils::unwrapResult(ffi::builder_build(engine_builder), "builder_build");
    transaction = DeltaLake::KernelUtils::unwrapResult(
        ffi::transaction(
            DeltaLake::KernelUtils::toDeltaString(kernel_helper->getTableLocation()),
            engine.get()),
        "transaction");

    write_context = ffi::get_write_context(transaction.get());
    write_path = std::unique_ptr<std::string>(
        static_cast<std::string *>(
            ffi::get_write_path(write_context.get(), DeltaLake::KernelUtils::allocateString)));

    LOG_TEST(log, "Write path: {}", *write_path);
}

void DeltaLakeStorageSink::consume(Chunk & chunk)
{
    if (isCancelled())
        return;

    chunks.push_back(chunk.clone());
}

void DeltaLakeStorageSink::onFinish()
{
    if (isCancelled() || chunks.empty())
        return;

    std::shared_ptr<arrow::Table> arrow_table;
    const size_t columns_num = chunks[0].getNumColumns();
    if (!ch_column_to_arrow_column)
    {
        ch_column_to_arrow_column = std::make_unique<CHColumnToArrowColumn>(
            getHeader(),
            "Arrow",
            CHColumnToArrowColumn::Settings
            {
                format_settings.arrow.output_string_as_string,
                format_settings.arrow.output_fixed_string_as_fixed_byte_array,
                format_settings.arrow.low_cardinality_as_dictionary,
                format_settings.arrow.use_signed_indexes_for_dictionary,
                format_settings.arrow.use_64_bit_indexes_for_dictionary
            });
    }
    ch_column_to_arrow_column->chChunkToArrowTable(arrow_table, chunks, columns_num);

    auto batch = arrow_table->CombineChunksToBatch();
    if (!batch.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
            "Failed to create chunks batch: {}", batch.status().ToString());

    struct ArrowArray c_array;
    struct ArrowSchema c_schema;
    arrow::Status status = arrow::ExportRecordBatch(**batch, &c_array, &c_schema);
    if (!status.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
            "Failed to export record batch: {}", status.ToString());

    const auto * ffi_array = reinterpret_cast<const ffi::FFI_ArrowArray *>(&c_array);
    const auto * ffi_schema = reinterpret_cast<const ffi::FFI_ArrowSchema *>(&c_schema);

    auto write_p = std::unique_ptr<std::string>(
        static_cast<std::string *>(ffi::get_write_path(write_context.get(), DeltaLake::KernelUtils::allocateString)));
    UNUSED(write_p);

    KernelEngineData engine_data = DeltaLake::KernelUtils::unwrapResult(
        ffi::get_engine_data(*ffi_array, ffi_schema, engine.get()),
        "get_engine_data");

    ffi::add_files(transaction.get(), engine_data.release());
    auto version = DeltaLake::KernelUtils::unwrapResult(ffi::commit(transaction.release(), engine.get()), "commit");

    LOG_TEST(log, "Commit version: {}", version);
}

}
