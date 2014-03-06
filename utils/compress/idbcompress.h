/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/** @file */

#ifndef IDBCOMPRESS_H__
#define IDBCOMPRESS_H__

#include <unistd.h>
#ifdef __linux__
#include <sys/types.h>
#endif
#include <vector>
#include <utility>

#if defined(_MSC_VER) && defined(xxxIDBCOMP_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace compress
{

typedef std::pair<uint64_t,uint64_t> CompChunkPtr;
typedef std::vector<CompChunkPtr> CompChunkPtrList;

class IDBCompressInterface
{
public:
	static const unsigned int HDR_BUF_LEN            = 4096;
	static const unsigned int UNCOMPRESSED_INBUF_LEN = 512 * 1024 * 8;

	// error codes from uncompressBlock()
	static const int ERR_OK = 0;
	static const int ERR_CHECKSUM = -1;
	static const int ERR_DECOMPRESS = -2;
	static const int ERR_BADINPUT = -3;
	static const int ERR_BADOUTSIZE = -4;

	/**
	* When IDBCompressInterface object is being used to compress a chunk, this
	* construct can be used to specify the padding added by padCompressedChunks
	*/
	EXPORT explicit IDBCompressInterface(unsigned int numUserPaddingBytes=0);

	/**
	 * dtor
	 */
	EXPORT virtual ~IDBCompressInterface();

	/**
	 * see if the algo is available in this lib
	 */
	EXPORT bool isCompressionAvail(int compressionType = 0) const;

	/**
	* Compresses specified "in" buffer of length "inLen" bytes.
	* Compressed data and size are returned in "out" and "outLen".
	* "out" should be sized using maxCompressedSize() to allow for incompressible data.
	* Returns 0 if success.
	*/
	EXPORT int compressBlock(const char* in,
		const size_t   inLen,
		unsigned char* out,
		unsigned int&  outLen) const;

	/**
 	* outLen must be initialized with the size of the out buffer before calling uncompressBlock.
 	* On return, outLen will have the number of bytes used in out.
 	*/
	EXPORT int uncompressBlock(const char* in, const size_t inLen, unsigned char* out,
		unsigned int& outLen) const;

	/**
	 * This fcn wraps whatever compression algorithm we're using at the time, and
	 * is not specific to blocks on disk.
	 */
	EXPORT int compress(const char *in, size_t inLen, char *out, size_t *outLen) const;

	/**
	 * This fcn wraps whatever compression algorithm we're using at the time, and
	 * is not specific to blocks on disk.  The caller needs to make sure out is big
	 * enough to contain the output by using getUncompressedSize().
	 */
	EXPORT int uncompress(const char *in, size_t inLen, char *out) const;

	/**
	* Initialize header buffer at start of compressed db file.
	*
	* @warning hdrBuf must be at least HDR_BUF_LEN*2 bytes
	*/
	EXPORT void initHdr(void* hdrBuf, int compressionType) const;

	/**
	* Initialize header buffer at start of compressed db file.
	*
	* @warning hdrBuf must be at least HDR_BUF_LEN bytes
	* @warning ptrBuf must be at least (hdrSize-HDR_BUF_LEN) bytes
	*/
	EXPORT void initHdr(void* hdrBuf, void* ptrBuf, int compressionType, int hdrSize) const;

	/**
	* Verify the passed in buffer contains a compressed db file header.
	*/
	EXPORT int verifyHdr(const void* hdrBuf) const;

	/**
	* Extracts list of compression pointers from the specified ptr buffer.
	* ptrBuf points to the pointer section taken from the headers.
	* chunkPtrs is a vector of offset, size pairs for the compressed chunks.
	* Returns 0 if success.
	*/
	EXPORT int getPtrList(const char* ptrBuf,
		const int ptrBufSize,
		CompChunkPtrList& chunkPtrs) const;

	/**
	* Extracts list of compression pointers from the specified header.
	* hdrBuf points to start of 2 buffer headers from compressed db file.
	* Overloaded for backward compatibility. For none dictionary columns.
	* Note: the pointer passed in is the beginning of the header,
	*       not the pointer section as above.
	*/
	EXPORT int getPtrList(const char* hdrBuf, CompChunkPtrList& chunkPtrs) const;

	/**
	* Return the number of chunk pointers contained in the specified ptr buffer.
	* ptrBuf points to the pointer section taken from the headers.
	*/
	EXPORT unsigned int getPtrCount(const char* ptrBuf,
		const int ptrBufSize) const;

	/**
	* Return the number of chunk pointers contained in the specified header.
	* hdrBuf points to start of 2 buffer headers from compressed db file.
	* For non-dictionary columns.
	*/
	EXPORT unsigned int getPtrCount(const char* hdrBuf) const;

	/**
	* Store vector of pointers into the specified buffer header's pointer section.
	*/
	EXPORT void storePtrs(const std::vector<uint64_t>& ptrs,
		void* hdrBuf,
		int ptrSectionSize) const;

	/**
	* Store vector of pointers into the specified buffer header.
	* Overloaded for backward compatibility. For none dictionary columns.
	* Note: the pointer passed in is the beginning of the header,
	*       not the pointer section as above.
	*/
	EXPORT void storePtrs(const std::vector<uint64_t>& ptrs, void* hdrBuf) const;

	/**
	* Calculates the chunk, and the block offset within the chunk, for the
	* specified block number.
	*/
	EXPORT void locateBlock(unsigned int block,
		unsigned int& chunkIndex,
		unsigned int& blockOffsetWithinChunk) const;

	/**
	 * Pads the specified compressed chunk to the nearest compressed chunk
	 * increment, by padding buf with 0's, and updating len accordingly.
	 * maxLen is the maximum size for buf.  nonzero return code means the
	 * result output buffer length is > than maxLen.
	 */
	EXPORT int padCompressedChunks(unsigned char* buf,
		unsigned int& len,
		unsigned int  maxLen ) const;

	/*
	 * Mutator methods for the block count in the file
	 */
	/**
	 * setBlockCount
	 */
	EXPORT void setBlockCount(void* hdrBuf, uint64_t count) const;

	/**
	 * getBlockCount
	 */
	EXPORT uint64_t getBlockCount(const void* hdrBuf) const;

	/*
	 * Mutator methods for the overall header size
	 */
	/**
	 * setHdrSize
	 */
	EXPORT void setHdrSize(void* hdrBuf, uint64_t size) const;

	/**
	 * getHdrSize
	 */
	EXPORT uint64_t getHdrSize(const void* hdrBuf) const;

	/**
	 * Mutator methods for the user padding bytes
	 */
	/**
	 * set numUserPaddingBytes
	 */
	EXPORT void numUserPaddingBytes(uint64_t num) { fNumUserPaddingBytes = num;  }

	/**
	 * get numUserPaddingBytes
	 */
	EXPORT uint64_t numUserPaddingBytes() const   { return fNumUserPaddingBytes; }

	/**
	 * Given an input, uncompressed block, what's the maximum possible output,
	 * compressed size?
	 */
	EXPORT static uint64_t maxCompressedSize(uint64_t uncompSize);

	/**
	 * Given a compressed block, returns the uncompressed size in outLen.
	 * Returns false on error, true on success.
	 */
	EXPORT static bool getUncompressedSize(char *in, size_t inLen, size_t *outLen);

protected:

private:
	//defaults okay
	//IDBCompressInterface(const IDBCompressInterface& rhs);
	//IDBCompressInterface& operator=(const IDBCompressInterface& rhs);

	unsigned int fNumUserPaddingBytes; // Num bytes to pad compressed chunks
};

#ifdef SKIP_IDB_COMPRESSION
inline IDBCompressInterface::IDBCompressInterface(unsigned int /*numUserPaddingBytes*/) {}
inline IDBCompressInterface::~IDBCompressInterface() {}
inline bool IDBCompressInterface::isCompressionAvail(int c) const { return (c == 0); }
inline int IDBCompressInterface::compressBlock(const char*,const size_t,unsigned char*,unsigned int&) const { return -1; }
inline int IDBCompressInterface::uncompressBlock(const char* in, const size_t inLen, unsigned char* out, unsigned int& outLen) const { return -1; }
inline int IDBCompressInterface::compress(const char* in, size_t inLen, char* out, size_t* outLen) const { return -1; }
inline int IDBCompressInterface::uncompress(const char* in, size_t inLen, char* out) const { return 0; }
inline void IDBCompressInterface::initHdr(void*,int) const {}
inline void IDBCompressInterface::initHdr(void*, void*, int,int) const {}
inline int IDBCompressInterface::verifyHdr(const void*) const { return -1; }
inline int IDBCompressInterface::getPtrList(const char*, const int, CompChunkPtrList&) const { return -1; }
inline int IDBCompressInterface::getPtrList(const char*, CompChunkPtrList&) const { return -1; }
inline unsigned int IDBCompressInterface::getPtrCount(const char*, const int) const { return 0; }
inline unsigned int IDBCompressInterface::getPtrCount(const char*) const { return 0; }
inline void IDBCompressInterface::storePtrs(const std::vector<uint64_t>&,void*,int) const {}
inline void IDBCompressInterface::storePtrs(const std::vector<uint64_t>&, void*) const {}
inline void IDBCompressInterface::locateBlock(unsigned int block,
	unsigned int& chunkIndex, unsigned int& blockOffsetWithinChunk) const {}
inline int IDBCompressInterface::padCompressedChunks(unsigned char* buf, unsigned int& len, unsigned int maxLen) const { return -1; }
inline void IDBCompressInterface::setBlockCount(void* hdrBuf, uint64_t count) const {}
inline uint64_t IDBCompressInterface::getBlockCount(const void* hdrBuf) const { return 0; }
inline void IDBCompressInterface::setHdrSize(void*, uint64_t) const {}
inline uint64_t IDBCompressInterface::getHdrSize(const void*) const { return 0; }
inline uint64_t IDBCompressInterface::maxCompressedSize(uint64_t uncompSize) { return uncompSize; }
inline bool IDBCompressInterface::getUncompressedSize(char* in, size_t inLen, size_t* outLen) { return false; }
#endif

}

#undef EXPORT

#endif

