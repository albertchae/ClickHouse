#pragma once
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelHelper.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLake/KernelPointerWrapper.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include "delta_kernel_ffi.hpp"


namespace DB
{
class DeltaLakeMetadataDeltaKernel;

class DeltaLakeStorageSink : public SinkToStorage
{
public:
    explicit DeltaLakeStorageSink(
        const DeltaLakeMetadataDeltaKernel & metadata,
        SharedHeader sample_block_,
        const FormatSettings & format_settings_);

    ~DeltaLakeStorageSink() override = default;

    String getName() const override { return "DeltaLakeStorageSink"; }

    void consume(Chunk & chunk) override;

    void onFinish() override;

private:
    using KernelTransaction = DeltaLake::KernelPointerWrapper<ffi::ExclusiveTransaction, ffi::free_transaction>;
    using KernelExternEngine = DeltaLake::KernelPointerWrapper<ffi::SharedExternEngine, ffi::free_engine>;
    using KernelWriteContext = DeltaLake::KernelPointerWrapper<ffi::SharedWriteContext, ffi::free_write_context>;
    using KernelEngineData = DeltaLake::KernelPointerWrapper<ffi::ExclusiveEngineData, ffi::free_engine_data>;

    const DeltaLake::KernelHelperPtr kernel_helper;
    const LoggerPtr log;
    const FormatSettings format_settings;

    KernelExternEngine engine;
    KernelTransaction transaction;
    KernelWriteContext write_context;
    std::unique_ptr<std::string> write_path;

    std::vector<Chunk> chunks;
    std::unique_ptr<CHColumnToArrowColumn> ch_column_to_arrow_column;
};

}
