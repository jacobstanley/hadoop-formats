{-# LANGUAGE ForeignFunctionInterface #-}

module Hadoop.Unsafe
    ( decodeSnappyBlock
    ) where

import           Control.Monad (unless)
import qualified Data.ByteString as B
import           Data.ByteString.Internal (ByteString(..))
import           Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import qualified Data.Vector as V
import qualified Data.Vector.Storable as SV
import           Data.Word (Word8)
import           Foreign.C
import           Foreign.ForeignPtr
import           Foreign.Marshal (alloca, finalizerFree)
import           Foreign.Ptr
import           Foreign.Storable
import           System.IO.Unsafe (unsafeDupablePerformIO)

------------------------------------------------------------------------

#include "decode.h"

foreign import ccall unsafe "hadoop_decode_snappy_block"
    hadoop_decode_snappy_block :: CInt
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

decodeSnappyBlock :: Int -> ByteString -> ByteString -> V.Vector ByteString
decodeSnappyBlock nRecs lengths values =
    unsafeDupablePerformIO $
    unsafeUseAsCStringLen lengths $ \(lPtr, lSize) ->
    unsafeUseAsCStringLen values  $ \(vPtr, vSize) ->
    alloca $ \blockPtr -> do

    err <- hadoop_decode_snappy_block (fromIntegral nRecs)
                                      lPtr (fromIntegral lSize)
                                      vPtr (fromIntegral vSize)
                                      blockPtr

    unless (err == 0)
           (error "decodeSnappyBlock: decode failed")

    block     <- peek blockPtr
    lengthPtr <- newForeignPtr finalizerFree (cLengthPtr block)
    dataPtr   <- newForeignPtr finalizerFree (cDataPtr block)

    let bs = PS dataPtr 0 (fromIntegral (cDataSize block))
        lv = SV.unsafeFromForeignPtr0 lengthPtr nRecs

    return (split bs lv)
  where
    split :: ByteString -> SV.Vector CSize -> V.Vector ByteString
    split bs lv = snd $ SV.foldl' go (bs, V.empty) lv

    go :: (ByteString, V.Vector ByteString) -> CSize -> (ByteString, V.Vector ByteString)
    go (bs, v) l = let (vbs, bs') = B.splitAt (fromIntegral l) bs
                   in (bs', v `V.snoc` vbs)
