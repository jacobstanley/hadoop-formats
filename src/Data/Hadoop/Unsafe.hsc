{-# LANGUAGE CPP #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Hadoop.Unsafe
    ( decodeSnappyBlock
    ) where

import           Control.Monad (unless, when)
import           Data.ByteString.Internal (ByteString(..))
import           Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import qualified Data.Vector.Storable as SV
import qualified Data.Vector.Unboxed as U
import           Data.Word (Word8)
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Marshal (alloca, finalizerFree)
import           Foreign.Ptr
import           Foreign.Storable
import           System.IO.Unsafe (unsafeDupablePerformIO)

import           Data.Hadoop.Writable

------------------------------------------------------------------------

#include "MachDeps.h"
#include "decode.h"

foreign import ccall unsafe "hadoop_decode_snappy_block"
    hadoop_decode_snappy_block :: CSize
                               -> CInt
                               -> CString -> CSize
                               -> CString -> CSize
                               -> Ptr CBlock
                               -> IO CInt

------------------------------------------------------------------------

#let alignment t = "%lu", (unsigned long)offsetof(struct {char x__; t (y__); }, y__)

data CBlock = CBlock
    { cLengthPtr :: Ptr CSize
    , cDataSize  :: CSize
    , cDataPtr   :: Ptr Word8
    } deriving (Eq, Show)

instance Storable CBlock where
   poke _ _    = error "CBlock.poke: not implemented"
   alignment _ = #alignment struct block
   sizeOf _    = #size struct block
   peek ptr    = do
       lPtr  <- (#peek struct block, lengths) ptr
       dSize <- (#peek struct block, data_size) ptr
       dPtr  <- (#peek struct block, data) ptr
       return (CBlock lPtr dSize dPtr)

------------------------------------------------------------------------

decodeSnappyBlock :: forall c a. Writable c a => Int -> ByteString -> ByteString -> c a
decodeSnappyBlock nRecs lengths values =
    unsafeDupablePerformIO $
    unsafeUseAsCStringLen lengths $ \(lPtr, lSize) ->
    unsafeUseAsCStringLen values  $ \(vPtr, vSize) ->
    alloca $ \blockPtr -> do

    err <- hadoop_decode_snappy_block (fromIntegral nRecs)
                                      recordSize
                                      lPtr (fromIntegral lSize)
                                      vPtr (fromIntegral vSize)
                                      blockPtr

    unless (err == 0)
           (error "decodeSnappyBlock: decode failed")

    CBlock{..} <- peek blockPtr

    when (cDataPtr == nullPtr)
         (error "decodeSnappyBlock: decoder returned null pointer to data array")

    dataPtr <- newForeignPtr finalizerFree cDataPtr
    let bytes = PS dataPtr 0 (fromIntegral cDataSize)

    case writableDecoder of
        LE16 f     -> return (f bytes)
        LE32 f     -> return (f bytes)
        LE64 f     -> return (f bytes)
        BE16 f     -> return (f bytes)
        BE32 f     -> return (f bytes)
        BE64 f     -> return (f bytes)
        Variable f -> do
            when (cLengthPtr == nullPtr)
                 (error "decodeSnappyBlock: decoder returned null pointer to length array")

            lengthPtr <- newForeignPtr finalizerFree cLengthPtr
            let lens  = U.convert . SV.map fromIntegral $ SV.unsafeFromForeignPtr0 lengthPtr nRecs

            return (f bytes lens)
  where
    writableDecoder = decoder :: Decoder (c a)

    -- Special record size encoding where negative numbers mean swap bytes
    recordSize = case writableDecoder of
        Variable _ -> 0
#ifdef WORDS_BIGENDIAN
        LE16 _ -> -2
        LE32 _ -> -4
        LE64 _ -> -8
        BE16 _ -> 2
        BE32 _ -> 4
        BE64 _ -> 8
#else
        LE16 _ -> 2
        LE32 _ -> 4
        LE64 _ -> 8
        BE16 _ -> -2
        BE32 _ -> -4
        BE64 _ -> -8
#endif
