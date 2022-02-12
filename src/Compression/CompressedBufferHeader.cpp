#include <Compression/CompressedBufferHeader.h>
#include <Compression/CompressionInfo.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>


namespace DB
{

void CompressedBufferHeader::read(ReadBuffer & in)
{
    readPODBinary(checksum, in);
    readPODBinary(compression_method, in);
    readPODBinary(compressed_size, in);
    readPODBinary(decompressed_size, in);
}

void CompressedBufferHeader::write(WriteBuffer & out)
{
    writePODBinary(checksum, out);
    writePODBinary(compression_method, out);
    writePODBinary(compressed_size, out);
    writePODBinary(decompressed_size, out);
}

}
