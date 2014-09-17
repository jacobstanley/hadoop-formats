{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Data.Hadoop.SequenceFile
    ( Header(..)
    , RecordBlock(..)
    , pHeader
    , pRecordBlock
    , pTextWritable
    , pBytesWritable
    ) where

import           Control.Applicative ((<$>), (<*>))
import           Control.Monad (when, unless, replicateM)
import           Data.Attoparsec.ByteString (Parser)
import qualified Data.Attoparsec.ByteString as A
import           Data.Bits ((.|.), xor, shiftL)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as B
import           Data.ByteString.Internal (ByteString(..))
import           Data.Int
import           Data.Text (Text)
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import           Data.Word
import           Foreign.ForeignPtr (withForeignPtr)
import           Foreign.Storable (Storable, peekByteOff)
import           System.IO.Unsafe (unsafeDupablePerformIO)
import           Text.Printf (printf)

import           Data.Hadoop.Unsafe

------------------------------------------------------------------------

#include "MachDeps.h"

------------------------------------------------------------------------

data Header = Header
    { keyType         :: !Text
    , valueType       :: !Text
    , compressionType :: !Text
    , metadata        :: ![(Text, Text)]
    , sync            :: !MD5
    } deriving (Show)

newtype MD5 = MD5 ByteString
    deriving (Eq)

data RecordBlock = RecordBlock
    { keys       :: !(V.Vector ByteString)
    , values     :: !(V.Vector ByteString)
    }

------------------------------------------------------------------------

pHeader :: Parser Header
pHeader = do
    magic <- A.take 3
    when (magic /= "SEQ")
         (fail "not a sequence file")

    version <- A.anyWord8
    when (version /= 6)
         (fail $ "unknown version: " ++ show version)

    keyType   <- pTextWritable
    valueType <- pTextWritable

    compression      <- anyBool
    blockCompression <- anyBool

    unless (compression && blockCompression)
           (fail "only block compressed files supported")

    compressionType <- pTextWritable

    unless (compressionType == "org.apache.hadoop.io.compress.SnappyCodec")
           (fail "only snappy compressed files supported")

    metadata <- pMetadata
    sync     <- anyMD5

    return Header{..}

pMetadata :: Parser [(Text, Text)]
pMetadata = do
    n <- fromIntegral <$> anyWord32le
    replicateM n $ (,) <$> pTextWritable <*> pTextWritable

------------------------------------------------------------------------

pRecordBlock :: MD5 -> Parser RecordBlock
pRecordBlock sync = do
    escape <- anyWord32le
    when (escape /= 0xffffffff)
         (fail $ "file corrupt, expected to find sync escape " ++
                 "<0xffffffff> but was " ++ printf "<0x%0x>" escape)

    sync' <- anyMD5
    when (sync /= sync')
         (fail $ "file corrupt, expected to find sync marker " ++
                 "<" ++ show sync ++ "> but was <" ++ show sync' ++ ">")

    cNumRecords   <- anyVInt
    cKeyLengths   <- vintPrefixedBytes
    cKeys         <- vintPrefixedBytes
    cValueLengths <- vintPrefixedBytes
    cValues       <- vintPrefixedBytes

    let keys   = decodeSnappyBlock cNumRecords cKeyLengths   cKeys
        values = decodeSnappyBlock cNumRecords cValueLengths cValues

    return RecordBlock{..}

------------------------------------------------------------------------

pTextWritable :: Parser Text
pTextWritable = T.decodeUtf8 <$> vintPrefixedBytes
{-# INLINE pTextWritable #-}

pBytesWritable :: Parser ByteString
pBytesWritable = A.take . fromIntegral =<< anyWord32be
{-# INLINE pBytesWritable #-}

------------------------------------------------------------------------

vintPrefixedBytes :: Parser ByteString
vintPrefixedBytes = A.take =<< anyVInt
{-# INLINE vintPrefixedBytes #-}

anyBool :: Parser Bool
anyBool = (/= 0) <$> A.anyWord8
{-# INLINE anyBool #-}

anyMD5 :: Parser MD5
anyMD5 = MD5 <$> A.take 16
{-# INLINE anyMD5 #-}

anyVInt :: Parser Int
anyVInt = fromIntegral <$> anyVInt64
{-# INLINE anyVInt #-}

anyVInt64 :: Parser Int64
anyVInt64 = withFirst . fromIntegral =<< A.anyWord8
  where
    withFirst :: Int8 -> Parser Int64
    withFirst x | size == 1 = return (fromIntegral x)
                | otherwise = fixupSign . B.foldl' go 0 <$> A.take (size - 1)
      where
        go :: Int64 -> Word8 -> Int64
        go i b = (i `shiftL` 8) .|. fromIntegral b

        size | x >= -112 = 1
             | x <  -120 = fromIntegral (-119 - x)
             | otherwise = fromIntegral (-111 - x)

        fixupSign v = if isNegative then v `xor` (-1) else v

        isNegative = x < -120 || (x >= -112 && x < 0)
{-# INLINE anyVInt64 #-}

------------------------------------------------------------------------

#ifdef WORDS_BIGENDIAN

anyWord32le :: Parser Word32
anyWord32le = anyWord32swap
{-# INLINE anyWord32le #-}

anyWord32be :: Parser Word32
anyWord32be = anyWord32host
{-# INLINE anyWord32be #-}

#else

anyWord32le :: Parser Word32
anyWord32le = anyWord32host
{-# INLINE anyWord32le #-}

anyWord32be :: Parser Word32
anyWord32be = anyWord32swap
{-# INLINE anyWord32be #-}

#endif

anyWord32host :: Parser Word32
anyWord32host = peekBS <$> A.take 4
{-# INLINE anyWord32host #-}

anyWord32swap :: Parser Word32
anyWord32swap = byteSwap32 <$> anyWord32host
{-# INLINE anyWord32swap #-}

peekBS :: Storable a => ByteString -> a
peekBS (PS fp off _) = unsafeDupablePerformIO $ withForeignPtr fp $ \ptr -> peekByteOff ptr off
{-# INLINE peekBS #-}

------------------------------------------------------------------------

instance Show MD5 where
    show (MD5 bs) = printf "%0x%0x%0x%0x%0x%0x"
                           (bs `B.index` 0)
                           (bs `B.index` 1)
                           (bs `B.index` 2)
                           (bs `B.index` 3)
                           (bs `B.index` 4)
                           (bs `B.index` 5)
